import json
import urllib.request
import boto3
from decimal import Decimal
from datetime import datetime
import urllib
import csv
import codecs
import os
import string
import random
import time
import base64


class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)

def hello(event, context):
    body = {
        "message": "We are Neil and this is the hello world",
    }
    print("Show event" + json.dumps(event))
    print("I added a little print here for debugging")

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response

def create_one_product(event, context):
    body = json.loads(event["body"], parse_float=Decimal)
    table_name = "product-neil"
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    table = dynamodb.Table(table_name)

    table.put_item(Item=body)

    response = {"statusCode": 200, "body": json.dumps(body, cls=DecimalEncoder)}

    sqs = boto3.resource('sqs', region_name='ap-southeast-2')
    queue = sqs.get_queue_by_name(QueueName='product-queue-neil')

    queue.send_message(MessageBody=json.dumps(body, cls=DecimalEncoder))

    logs_client = boto3.client('logs', region_name='ap-southeast-2')

    LOG_GROUP = "ProductEventLogGroup"
    LOG_STREAM = "ProductEventStream"

    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass
    
    log_event = {
        "event": "product_created",
        "pid": body.get("product_id"),
        "data": body
    }
    
    logs_client.put_log_events(
        logGroupName=LOG_GROUP,
        logStreamName=LOG_STREAM,
        logEvents=[
            {
                'timestamp': int(time.time() * 1000),
                'message': json.dumps(log_event, cls=DecimalEncoder)
            }
        ]
    )
    
    print("Product creation event logged in CloudWatch.")
    
    return response
		
def get_one_product(event, context):
    body = {
        "message": "I'm getting the product ID",
        "input": event,
    }
    
    product_id = event.get("pathParameters", {}).get("product_id")
    product_table_name = "product-neil"
    inventory_table_name = "ProductInventory"
    
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    product_table = dynamodb.Table(product_table_name)
    inventory_table = dynamodb.Table(inventory_table_name)
    
    # Get product details
    product_response = product_table.get_item(Key={'product_id': product_id})
    if "Item" not in product_response:
        return {"statusCode": 404, "body": json.dumps({"message": "Product not found"})}
    
    product = product_response["Item"]
    
    # Query inventory to sum stock quantity
    inventory_response = inventory_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('product_id').eq(product_id)
    )
    
    total_quantity = 0
    for item in inventory_response.get("Items", []):
        total_quantity += item.get("quantity", 0)
    
    # Add total stock count to product details
    product["total_stock"] = total_quantity
    
    return_body = {
        "Item": product,
        "total_stock": total_quantity,
        "status": "success"
    }
    
    response = {"statusCode": 200, "body": json.dumps(return_body, cls=DecimalEncoder)}
    
    return response
def delete_one_product(event, context):
    body = {
        "message": "I deleted the selected product ID",
        "input": event,
    }
    product_id = event.get("pathParameters", {}).get("product_id")
    table_name = "product-neil" # change this to your dynamodb table name
    
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    table = dynamodb.Table(table_name)
    
    return_body = {}
    return_body = table.delete_item(Key={
        'product_id': product_id
    })
    return_body["status"] = "successfully deleted"
    response = {"statusCode": 200, "body": json.dumps(return_body, cls=DecimalEncoder)}
    
    return response
def update_product(event, context):
    body = {
        "message": "I updated the selected product ID",
        "input": event,
    }
    
    product_data = json.loads(event.get("body", "{}"))
    product_item = product_data.get("product_item")  
    product_value = product_data.get("product_value")  
    product_id = event.get("pathParameters", {}).get("product_id")
    
    table_name = "product-neil" 
    
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    table = dynamodb.Table(table_name)
    
    try:
        if not product_item or not product_value:
            raise ValueError("Both 'product_item' and 'product_value' must be provided.")
        
        # Update the item in DynamoDB
        response = table.update_item(
            Key={'product_id': product_id},
            UpdateExpression=f'SET {product_item} = :val1',
            ExpressionAttributeValues={':val1': product_value},
            ReturnValues="ALL_NEW"  # Optionally, return the updated item
        )
        
        # Retrieve the updated item from the response
        updated_item = response.get("Attributes", {})
        
        # Prepare the response body
        return_body = {
            "message": "Product updated successfully",
            "updated_item": updated_item,
            "status": "successfully updated"
        }
        
        # Return a successful response
        response = {"statusCode": 200, "body": json.dumps(return_body, cls=DecimalEncoder)}
        return response

    except Exception as e:
        # If an error occurs, return an error response with the message
        error_response = {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
        return error_response

def add_stocks_to_product(event, context):
    product_id = event.get("pathParameters", {}).get("product_id")
    quantity = json.loads(event.get("body", "{}")).get("quantity")
    remarks = json.loads(event.get("body", "{}")).get("remarks", "No remarks")
    
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    inventory_table = dynamodb.Table("ProductInventory")
    
    # Get current timestamp for datetime
    timestamp = datetime.now().isoformat()
    
    # Add a new stock entry to ProductInventory
    response = inventory_table.put_item(
        Item={
            "product_id": product_id,
            "datetime": timestamp,
            "quantity": quantity,
            "remarks": remarks
        }
    )
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Stock updated success",
            "product_id": product_id,
            "quantity_added": quantity,
            "remarks": remarks
        })
    }
def batch_create_products(event, context):
    print("file uploaded trigger")
    print(event)
    
    print("Extract file location from event payload")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
     # Define the target directory and filename path
    localDirectory = '/tmp/for_create'
    localFilename = os.path.join(localDirectory, key.split('/')[-1])
    
    # Debug: Print the local paths being used
    print(f"Target directory: {localDirectory}")
    print(f"Target file path: {localFilename}")

    # Ensure the subdirectory exists
    if not os.path.exists(localDirectory):
        print(f"Directory does not exist. Creating directory: {localDirectory}")
        os.makedirs(localDirectory)
    else:
        print(f"Directory already exists: {localDirectory}")

    localFilename = f'/tmp/{key}'
    s3_client = boto3.client('s3', region_name='ap-southeast-2')
    
    print("downloaded file to /tmp folder")
    s3_client.download_file(bucket, key, localFilename)
    
    print("reading CSV file and looping it over...")
    if "for_create/" in key:
        with open(localFilename, 'r') as f:
            csv_reader = csv.DictReader(f)
            required_keys = ["product_id", "product_name", "price", "quantity"]
            table_name = "product-neil" # change this to your dynamodb table name
            dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
            table = dynamodb.Table(table_name)
            for row in csv_reader:
                if all(key in row for key in required_keys):
                    table.put_item(
                        Item=row
                    )
    
    print("All done!")
    return {}
def batch_delete_products(event, context):
    print("file uploaded trigger")
    print(event)
    
    print("Extract file location from event payload")
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
     # Define the target directory and filename path
    localDirectory = '/tmp/for_delete'
    localFilename = os.path.join(localDirectory, key.split('/')[-1])
    
    print(f"Target directory: {localDirectory}")
    print(f"Target file path: {localFilename}")

    # Ensure the subdirectory exists
    if not os.path.exists(localDirectory):
        print(f"Directory does not exist. Creating directory: {localDirectory}")
        os.makedirs(localDirectory)
    else:
        print(f"Directory already exists: {localDirectory}")

    s3_client = boto3.client('s3', region_name='ap-southeast-2')
    
    print("downloaded file to /tmp folder")
    s3_client.download_file(bucket, key, localFilename)
    
    print("reading CSV file and looping it over...")
    if "for_delete/" in key:
        with open(localFilename, 'r') as f:
            csv_reader = csv.DictReader(f)
            required_keys = ["product_id"]
            table_name = "product-neil" 
            dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
            table = dynamodb.Table(table_name)
            for row in csv_reader:
                if "product_id" in row:  # Ensure product_id is present in the row
                    product_id = row["product_id"]
                    print(f"Attempting to delete product with ID: {product_id}")
                    table.delete_item(
                        Key={
                            "product_id": product_id  # Assuming the primary key is product_id
                        }
                    )
                    print(f"Successfully deleted product with ID: {product_id}")

    print("All done!")
    return {}
def generate_code(prefix, string_length):
  letters = string.ascii_uppercase
  return prefix + ''.join(random.choice(letters) for i in range(string_length))

def receive_message_from_sqs(event, context):
    print("file uploaded trigger")
    print(event)
    
    fieldnames=["product_id", "product_name", "price", "quantity"]
    
    file_randomized_prefix = generate_code("pycon_", 8)
    file_name = f'/tmp/product_created_{file_randomized_prefix}.csv'
    bucket = "queue-s3bucket-neil"
    object_name = f'product_created_{file_randomized_prefix}.csv'
    
    
    with open(file_name, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for payload in event["Records"]:
            json_payload = json.loads(payload["body"])
            writer.writerow(json_payload)

   
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
        
    print("All done!")
    return {}

def upload_s3event(event, context):
    s3_client = boto3.client('s3')
    S3_BUCKET_NAME = 'products-s3bucket-neil'
    process = json.loads(event.get("body", "{}")).get("process")
    # Extract the file content and metadata from the incoming request
    try:
        # Get the file content (Base64 encoded, if that's how it's sent)
        file_data = event['body']
        
        # Check if the content is Base64 encoded
        if event.get('isBase64Encoded', False):
            file_data = base64.b64decode(file_data)  # Decode Base64 data if it's encoded
        
        # Ensure that file_data is not empty after decoding
        if not file_data:
            raise ValueError("File data is empty after decoding.")
        
        # Extract the file name from the headers or the request body (if passed)
        upload_url = event['headers'].get('file-name', 'default_file_name.txt')  # You may pass filename in headers
        download_url = urllib.request.urlopen(upload_url)
        parsed_url = urllib.parse.urlparse(upload_url)
    
        # Extract the path from the URL
        path = parsed_url.path
        if 'file_' in path:
            file_name = path.split('file_')[-1]
        # Get the file name from the path (after the last '/')
        file_name = file_name.split('_')[-1]
        file_name = file_name.split('/')[-1]

        
        file_data = download_url.read()
    
        s3_file_key = f'{process}/{file_name}'  # Store the file in the "for_create" folder

        # Check if the S3 key length exceeds the allowed limit
        MAX_KEY_LENGTH = 1024
        if len(s3_file_key) > MAX_KEY_LENGTH:
            raise ValueError(f"S3 key is too long: {len(s3_file_key)} characters.")

        # Log the size of the file to ensure it's not empty
        print(f"Uploading file with size: {len(file_data)} bytes")

        # Upload file to S3
        response = s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_file_key,
            Body=file_data
        )

        # Return a success message with the S3 file URL
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'File uploaded successfully!',
                'file_url': f'https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_file_key}'
            })
        }

    except Exception as e:
        # In case of an error, return the error message
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to upload the file.',
                'error': str(e)
            })
        }