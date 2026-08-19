import json
import re
import os
import urllib.parse
import boto3
import psycopg

s3_client = boto3.client('s3')
DATABASE_URL = os.environ.get('DATABASE_URL')


def parse_sensor_message(message_str):
    print(f"Processing message: {message_str}")

    data = {}
    
    # Extrai TEMP, RPM e PRESSURE usando Expressão Regular
    temp_match = re.search(r'TEMP=([\d.]+)', message_str)
    rpm_match = re.search(r'RPM=(\d+)', message_str)
    press_match = re.search(r'PRESSURE=([\d.]+)', message_str)
    status_match = re.search(r'\b(OK|FAIL|WARN)\b', message_str)
    
    data['temp'] = float(temp_match.group(1)) if temp_match else None
    data['rpm'] = int(rpm_match.group(1)) if rpm_match else None
    data['pressure'] = float(press_match.group(1)) if press_match else None
    
    # Extrai o status final (ex: OK, FAIL, etc.)
    data['status'] = status_match.group(1) if status_match else 'UNKNOWN'
    
    return data


def resolve_sns_sent(json_data):
    """Le o resultado REAL do envio do SNS, gravado pela lambda1 no proprio payload.
    'TRUE'/'FALSE' vem do retorno real do sns_client.publish (sucesso ou ClientError).
    'NA' (ou ausente) significa que o alerta nao era aplicavel (status OK) -> grava NULL."""
    raw_flag = str(json_data.get('sns_sent', 'NA')).upper()
    if raw_flag == 'TRUE':
        return True
    if raw_flag == 'FALSE':
        return False
    return None

def lambda_handler(event, context):
    print("Processing payload:")
    print(event)
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = json.loads(s3_object['Body'].read().decode('utf-8'))
        
        # Extrai os dados brutais do JSON
        device_id = json_data.get('device_id')
        raw_message = json_data.get('message', '')
        
        # Converte a string "TEMP=21 RPM=1616..." em valores numéricos
        metrics = parse_sensor_message(raw_message)
        
        # sns_sent reflete o resultado REAL do publish no SNS (gravado pela lambda1),
        # não é mais deduzido por regex sobre o texto da mensagem.
        metrics['sns_sent'] = resolve_sns_sent(json_data)
        
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL environment variable is not set")

        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                insert_query = """
                    INSERT INTO iot_readings 
                        (device_id, temperatura, rpm, pressure, status, sns_sent, s3_bucket, s3_key, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cur.execute(insert_query, (
                    device_id,
                    metrics['temp'],
                    metrics['rpm'],
                    metrics['pressure'],
                    metrics['status'],
                    metrics['sns_sent'],
                    bucket,
                    key,
                    json.dumps(json_data) # Salva o JSON completo como JSONB
                ))
                
        return {'statusCode': 200, 'body': 'Sucesso'}

    except Exception as e:
        print(f"Erro: {str(e)}")
        raise e