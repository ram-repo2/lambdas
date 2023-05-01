import boto3
import os
import json
import logging

# Define a custom logger
logger = logging.getLogger('SQS_Onboarding')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Define a stream handler to output logs to stdout
stdout_handler = logging.StreamHandler()
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(formatter)
logger.addHandler(stdout_handler)

# Define a stream handler to output error logs to stderr
stderr_handler = logging.StreamHandler()
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(formatter)
logger.addHandler(stderr_handler)

# Function to check if an SQS queue already exists
def queue_exists(queue_name):
    logger.info('Checking if SQS queue exists')
    try:
        sqs = boto3.client('sqs')
        response = sqs.get_queue_url(QueueName=queue_name)
        return True
    except:
        return False

# Function to create an SQS queue
def create_queue(queue_name):
    logger.info('Creating new SQS queue')
    sqs = boto3.client('sqs')
    response = sqs.create_queue(QueueName=queue_name)
    return response['QueueUrl']

# Function to delete an SQS queue
def delete_queue(queue_url):
    logger.info('Deleting SQS queue')
    sqs = boto3.client('sqs')
    sqs.delete_queue(QueueUrl=queue_url)

# Create or Delete sqs when action is Onboard/offboard
# Create or Delete sqs when action is Onboard/offboard

def sqs_action(restaurant_ids, action, environment):
    logger.info('Started SQS based on Action')
    QUEUE_NAMES = set()

    # Create the names of the SQS queues for each restaurant
    for restaurant_id in restaurant_ids:
        QUEUE_NAMES.add(f'US-EAST-{environment}-{restaurant_id}-SQS-DEPLOYMENT')
        QUEUE_NAMES.add(f'CM-{environment}-{restaurant_id}-SQS-DEPLOYMENT')

    # Define the names of the SQS queues
    if action == 'OnboardCluster':
        # Check if the queues already exist
        queue_urls = []
        for queue_name in QUEUE_NAMES:
            if not queue_exists(queue_name):
                queue_url = create_queue(queue_name)
                queue_urls.append(queue_url)
            else:
                sqs = boto3.client('sqs')
                queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
                queue_urls.append(queue_url)
        return f"Created queues with URLs: {queue_urls}"
    elif action == 'OffboardCluster':
        # Delete the queues
        for queue_name in QUEUE_NAMES:
            if queue_exists(queue_name):
                sqs = boto3.client('sqs')
                queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
                delete_queue(queue_url)
        return f"Deleted queues: {QUEUE_NAMES}"
    else:
        return 'Invalid action specified'


# Function to send SNS to Trigger onboarding Lambda
def publish_sns_message(topic_arn, message, attribute_name, attribute_value):
    logger.info('sending SNS to trigger onboarding lambda')
    sns = boto3.client('sns')
    message_attributes = {
        attribute_name: {
            'DataType': 'String',
            'StringValue': attribute_value
        }
    }
    logger.info(message_attributes)
    response = sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
        MessageStructure='json',
        MessageAttributes=message_attributes
    )
    return response

# Define a Lambda function handler
def lambda_handler(event, context):
    try:
        aws_region = os.environ.get("REGION", "us-east-1")
        data = event["body"]
        if type(data) is str:
            data = json.loads(data)
        logger.info('Event body "{}"'.format(data))
        action = data["action"]
        restaurant_ids = data["restaurant_ids"]
        topic_arn = 'arn:aws:sns:us-east-1:308002896215:Onboard'
        environment = data["environment"]

        # Define the names of the SQS queues
        if action == 'OnboardCluster':
            message = sqs_action(restaurant_ids, action, environment)
            return message
        elif action == 'OffboardCluster':
            # Delete the queues
            message = sqs_action(restaurant_ids, action, environment)
            return message
        else:
            message = "Invalid action specified"
            return message
                
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise e
