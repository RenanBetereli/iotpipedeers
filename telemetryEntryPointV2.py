import json
import os
import re
import boto3
import uuid
from botocore.exceptions import ClientError
from datetime import datetime, timezone

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Variáveis globais de ambiente sem fallback sensível
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'jdf-data-lake-renan')
BASE_PREFIX = os.environ.get('BASE_PREFIX', 'landing/').strip('/')
TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '').strip()

def sanitize_device_id(raw_id: str) -> str:
    """Remove caracteres especiais, espaços, pontos e barras, mantendo apenas alfanuméricos."""
    if not raw_id:
        return ""
    return re.sub(r'[^a-zA-Z0-9]', '', str(raw_id))

def should_send_fail_alert(message: str) -> bool:
    """Dispara alerta apenas quando FAIL aparece como token da mensagem."""
    if not isinstance(message, str):
        return False
    tokens = [t.upper() for t in re.split(r'[\s;]+', message.strip()) if t]
    return 'FAIL' in tokens

def publish_sns_fail_alert(device_id: str, message: str, s3_key: str) -> tuple[bool, str | None]:
    """Publica alerta SNS para telemetria com FAIL e retorna status/erro."""
    if not TOPIC_ARN:
        reason = 'SNS_TOPIC_ARN não configurado.'
        print(f'{reason} Alerta FAIL não enviado.')
        return False, reason

    sns_message = {
        'alert_type': 'IOT_TELEMETRY_FAIL',
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'device_id': device_id,
        'message': message,
        's3_key': s3_key
    }

    try:
        response = sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject='Alerta IoT: FAIL detectado',
            Message=json.dumps(sns_message, ensure_ascii=False),
            MessageAttributes={
                'severity': {
                    'DataType': 'String',
                    'StringValue': 'HIGH'
                },
                'status': {
                    'DataType': 'String',
                    'StringValue': 'FAIL'
                },
                'device_id': {
                    'DataType': 'String',
                    'StringValue': device_id
                }
            }
        )
        print(f"Mensagem SNS enviada com sucesso. MessageId: {response.get('MessageId')}")
        return True, None
    except ClientError as e:
        reason = e.response['Error']['Message']
        print(f"Erro ao enviar para o SNS: {reason}")
        return False, reason

def lambda_handler(event, context):
    print("Event received:")
    print(event)
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
    }
    
    http_method = event.get('httpMethod', 'POST')

    # Trata requisição Preflight CORS
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'CORS preflight OK'})
        }

    try:
        # =========================================================================
        # FLUXO POST: Ingestão e Validação de Telemetria
        # =========================================================================
        if http_method == 'POST':
            body_str = event.get('body', '{}')
            
            if isinstance(body_str, str):
                payload = json.loads(body_str)
            else:
                payload = body_str

            # 1. Validação estrita: OBRIGATÓRIO ter device_id E message
            raw_device_id = payload.get('device_id')
            message = payload.get('message')

            if not raw_device_id or message is None:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'error': 'Campos obrigatórios ausentes: \'device_id\' e \'message\' devem ser informados.'
                    }, ensure_ascii=False)
                }

            # 2. Sanitização e validação de tamanho do device_id (Max 30 chars)
            clean_device_id = sanitize_device_id(raw_device_id)

            if len(clean_device_id) == 0:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'error': 'device_id inválido após sanitização (deve conter letras e/ou números).'
                    }, ensure_ascii=False)
                }

            if len(clean_device_id) > 30:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'error': 'device_id excede o limite máximo permitido de 30 caracteres.'
                    }, ensure_ascii=False)
                }

            # 3. Definição do caminho no S3 (Consolidado na raiz do dia)
            now = datetime.now(timezone.utc)
            year = now.strftime('%Y')
            month = now.strftime('%m')
            day = now.strftime('%d')
            timestamp_ms = int(now.timestamp() * 1000)
            unique_id = str(uuid.uuid4())[:8]

            prefix = f"{BASE_PREFIX}/" if BASE_PREFIX else ""
            
            # Estrutura sem subpastas extras de device: landing/ano=YYYY/mes=MM/dia=DD/...
            filename = f"{clean_device_id}_{timestamp_ms}_{unique_id}.json"
            s3_key = f"{prefix}ano={year}/mes={month}/dia={day}/{filename}"

            # 4. Publica o alerta SNS (se aplicável) ANTES de gravar no S3, para poder
            # persistir o resultado REAL do envio (retorno do publish/ClientError) no payload.
            fail_alert_sent = False
            fail_alert_reason = None
            if should_send_fail_alert(message):
                fail_alert_sent, fail_alert_reason = publish_sns_fail_alert(clean_device_id, message, s3_key)
                sns_sent_flag = 'TRUE' if fail_alert_sent else 'FALSE'
            else:
                sns_sent_flag = 'NA'

            # Payload original: mantém toda a informação compactada no campo message,
            # além do status REAL (não deduzido) do envio do alerta SNS.
            original_payload = {
                'device_id': clean_device_id,
                'message': message,
                'sns_sent': sns_sent_flag
            }

            # 5. Salva o arquivo no S3
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(original_payload, ensure_ascii=False),
                ContentType='application/json',
                Metadata={
                    'device_id': clean_device_id,
                    'ingestion_timestamp': str(timestamp_ms)
                }
            )

            # 6. Resposta sem expor infraestrutura/bucket S3
            return {
                'statusCode': 201,
                'headers': cors_headers,
                'body': json.dumps({
                    'status': 'success',
                    'message': 'Dados de telemetria recebidos e armazenados com sucesso.',
                    'fail_alert_sent': fail_alert_sent,
                    'fail_alert_reason': fail_alert_reason
                }, ensure_ascii=False)
            }

        # Método HTTP não suportado
        else:
            return {
                'statusCode': 405,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Método {http_method} não suportado.'})
            }

    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'JSON enviado no corpo da requisição é inválido.'}, ensure_ascii=False)
        }
    except Exception as e:
        print(f"Erro interno de execução no processamento: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Erro interno no processamento da requisição.'}, ensure_ascii=False)
        }