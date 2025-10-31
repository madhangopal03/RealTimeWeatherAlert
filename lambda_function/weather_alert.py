import json
import boto3
from datetime import datetime
import uuid

# Initialize AWS
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WeatherLogs')
sns = boto3.client('sns')
TOPIC_ARN = 'arn:aws:sns:us-east-1:904233131624:WeatherAlerts'

def lambda_handler(event, context):
    try:
        # Get event data
        city = event.get('City', 'Unknown')
        temperature = event.get('Temperature', 32)
        condition = event.get('Condition', 'Sunny')
        timestamp = datetime.now().isoformat()
        log_id = str(uuid.uuid4())

        # Store record
        table.put_item(
            Item={
                'LogID': log_id,
                'City': city,
                'Temperature': temperature,
                'Condition': condition,
                'Timestamp': timestamp
            }
        )

        # Fetch only recent 10 records
        response = table.scan(Limit=10)
        items = response.get('Items', [])

        # Create email content
        email_message = "🌦️ Weather Log Summary (Latest 10)\n\n"
        for item in items:
            email_message += (
                f"City: {item.get('City', 'N/A')}\n"
                f"Condition: {item.get('Condition', 'N/A')}\n"
                f"Temperature: {item.get('Temperature', 'N/A')}°C\n"
                f"Timestamp: {item.get('Timestamp', 'N/A')}\n"
                f"{'-'*40}\n"
            )

        # Send via SNS
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject='Weather Logs Summary (Latest 10)',
            Message=email_message
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data stored and email sent successfully',
                'LogID': log_id
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
