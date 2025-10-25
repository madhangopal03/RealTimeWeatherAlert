import json
import boto3
from datetime import datetime
import uuid

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WeatherLogs')

# Initialize SNS
sns = boto3.client('sns')
TOPIC_ARN = 'arn:aws:sns:us-east-1:904233131624:WeatherAlerts'

def lambda_handler(event, context):
    try:
        temperature = event.get('Temperature', 32)
        condition = event.get('Condition', 'Sunny')
        timestamp = datetime.now().isoformat()
        log_id = str(uuid.uuid4())

        # Store in DynamoDB
        table.put_item(
            Item={
                'LogID': log_id,
                'Temperature': temperature,
                'Condition': condition,
                'Timestamp': timestamp
            }
        )

        # Send alert for severe conditions
        if condition in ['Rainy', 'Storm', 'Snow']:
            message = f"Weather Alert!\nCondition: {condition}\nTemperature: {temperature}°C\nTime: {timestamp}"
            sns.publish(
                TopicArn=TOPIC_ARN,
                Message=message,
                Subject='Weather Alert'
            )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data stored and alert sent if necessary',
                'data': {
                    'LogID': log_id,
                    'Temperature': temperature,
                    'Condition': condition,
                    'Timestamp': timestamp
                }
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
