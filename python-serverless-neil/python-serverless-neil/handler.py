import json
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