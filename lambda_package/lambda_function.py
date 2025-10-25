import json
import boto3
import requests
import os
from datetime import datetime
import uuid
from decimal import Decimal

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WeatherLogs')
sns = boto3.client('sns')
TOPIC_ARN = 'arn:aws:sns:us-east-1:904233131624:WeatherAlerts'  # Replace if needed

# Get API Key
API_KEY = os.environ['OPENWEATHER_API_KEY']

# List of cities to monitor automatically 🌦️
CITIES = ['Chennai', 'Mumbai', 'Delhi', 'Bangalore', 'Hyderabad']

def lambda_handler(event, context):
    results = []  # Store results for all cities

    try:
        for city in CITIES:
            # Build API URL for each city
            url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
            
            response = requests.get(url)
            data = response.json()

            # Extract weather details
            temperature = Decimal(str(data['main']['temp']))
            condition = data['weather'][0]['main']
            timestamp = datetime.now().isoformat()
            log_id = str(uuid.uuid4())

            # Store in DynamoDB
            table.put_item(
                Item={
                    'LogID': log_id,
                    'City': city,
                    'Temperature': temperature,
                    'Condition': condition,
                    'Timestamp': timestamp
                }
            )

            # Send alert if weather is severe
            if condition in ['Rain', 'Thunderstorm', 'Snow']:
                message = (
                    f"⚠️ Weather Alert for {city}!\n"
                    f"Condition: {condition}\n"
                    f"Temperature: {temperature}°C\n"
                    f"Time: {timestamp}"
                )
                sns.publish(
                    TopicArn=TOPIC_ARN,
                    Message=message,
                    Subject=f'Weather Alert - {city}'
                )

            # Append results for summary
            results.append({
                'City': city,
                'Temperature': float(temperature),
                'Condition': condition,
                'Timestamp': timestamp
            })

        # Return combined data for all cities
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data collected for all cities',
                'data': results
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
