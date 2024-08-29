import pymysql
import time
import boto3
import json
import os
import cv2
import numpy as np
from urllib.parse import unquote_plus
REGION = 'eu-central-1'
# Initialize the S3 client
s3 = boto3.client('s3')
s3resource = boto3.resource('s3')
paginator = s3.get_paginator('list_objects_v2')
db_config = {
        'host': 'xxx',
        'user': 'root',
        'password': 'xxx',
        'database': 'xxx'
    }
connection = pymysql.connect(**db_config)
# Connect to the MySQL database
def get_connection():
    global connection
    if connection and connection.open:
        return connection
    else:
        connection = pymysql.connect(**db_config)
        return connection

client = boto3.client('lambda')

def get_meta(source_bucket, source_key):

    # Define the input parameters that will be passed
    # on to the child function
    inputParams = {
        "bucket"   : source_bucket,
        "key"      : source_key
    }

    response = client.invoke(
        FunctionName = 'arn:aws:lambda:eu-central-1:xxxxx:function:test112-ExiftoolLambda-414TCIKaIRuB',
        InvocationType = 'RequestResponse',
        Payload = json.dumps(inputParams)
    )

    r = response['Payload']
    responseFromChild = r.read().decode("utf-8")
    return responseFromChild

def update_folder(parent_folder_id, folder):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to update folder
        sql = "UPDATE folders SET parent_folder_id = %s WHERE name = %s AND parent_folder_id IS NULL"

        # Execute the SQL query with the folder
        cursor.execute(sql, (parent_folder_id, folder))

        # Commit the transaction
        connection.commit()

        print("folder updated successfully!")

    except pymysql.Error as error:
        print("Failed to update data into MySQL table:", error)
def insert_folder(data, cursor):
    try:
        # Construct the SQL query to insert folder
        sql = "INSERT INTO folders (name, parent_folder_id) VALUES (%s, %s)"

        # Execute the SQL query with the folder
        cursor.execute(sql, data)

        # Commit the transaction
        connection.commit()

        print("folder inserted successfully!")

    except pymysql.Error as error:
        print("Failed to insert data into MySQL table:", error)
def delete_all_objects(folder_id):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to delete folder objects
        sql = "DELETE FROM images WHERE folder_id = %s"

        # Execute the SQL query with the data
        cursor.execute(sql, folder_id)

        # Commit the transaction
        connection.commit()

        print("Data deleted successfully!")

    except pymysql.Error as error:
        print("Failed to delete data into MySQL table:", error)
def delete_folder(result):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to delete folder
        sql = "DELETE FROM folders WHERE name = %s"
        # Execute the SQL query with the folder
        cursor.execute(sql, result)

        # Commit the transaction
        connection.commit()

        print("folder deleted successfully!")

    except pymysql.Error as error:
        print("Failed to delete data into MySQL table:", error)
def delete_child(source_key):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to delete folder
        sql = "DELETE FROM folders WHERE name LIKE %s"
        sk1 = source_key + '%'
        # Execute the SQL query with the folder
        cursor.execute(sql, sk1)

        # Commit the transaction
        connection.commit()

        print("folder deleted successfully!")

    except pymysql.Error as error:
        print("Failed to delete data into MySQL table:", error)
def get_id(result, cursor):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to query folder id
        sql = "SELECT id FROM folders WHERE name = %s"

        # Execute the SQL query with the data
        cursor.execute(sql, result)
        # Fetch the result if any
        result = cursor.fetchone()

        return result

        print("Data query successfully!")

    except pymysql.Error as error:
        print("Failed to query folder id:", error)
def delete_data(source_key):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to delete data
        sql = "DELETE FROM images WHERE imgKey = %s"

        # Execute the SQL query with the data
        cursor.execute(sql, source_key)

        # Commit the transaction
        connection.commit()

        print("Data deleted successfully!")

    except pymysql.Error as error:
        print("Failed to delete data into MySQL table:", error)
def insert_data(data, cursor):
    try:
        cursor = connection.cursor()
        # Construct the SQL query to insert data
        sql = "INSERT INTO images (url, thumb_url, meta_data, imgKey, folder_id, meta2) VALUES (%s, %s, %s, %s, %s, %s)"

        # Execute the SQL query with the data
        cursor.execute(sql, data)

        # Commit the transaction
        connection.commit()

        print("Data inserted successfully!")

    except pymysql.Error as error:
        print("Failed to insert data into MySQL table:", error)
def update_meta(meta, key):
    try:
        cursor = connection.cursor()

        # Construct the SQL query to update meta
        sql = "UPDATE images SET meta2 = %s WHERE imgKey = %s"

        # Execute the SQL query with the meta
        cursor.execute(sql, (meta, key))

        # Commit the transaction
        connection.commit()

        print("meta updated successfully!")

    except pymysql.Error as error:
        print("Failed to update data into MySQL table:", error)
def batch(source_bucket, prefix, cursor):
    pages = paginator.paginate(Bucket=source_bucket, Prefix=prefix, Delimiter='/')
    len_prefix = prefix.count('/')
    for page in pages:
        # Add entry to DB if not exist
        print(page)
        if 'CommonPrefixes' in page:
            for obj in page['CommonPrefixes']:
                print(obj["Prefix"])
                len_key = obj["Prefix"].count('/')
                folder = obj["Prefix"].rsplit('/', 1)[0]
                result = obj["Prefix"].rsplit('/', 2)[0]
                parent_folder_id = get_id(result, cursor)
                data = (folder, parent_folder_id)
                fid = get_id(folder, cursor)
                if fid is None:
                    insert_folder(data, cursor)
                    update_folder(parent_folder_id, folder)
                batch(source_bucket, obj["Prefix"], cursor)

def lambda_handler(event, context):
    # Check if 'Records' key exists in the event
    print("Received event: " + json.dumps(event, indent=2))
    if 'Records' not in event or not event['Records']:
        return {'statusCode': 400, 'body': 'No S3 event detected.'}
    with connection.cursor() as cursor:
        connection.ping()
        for record in event['Records']:
            source_bucket = record['s3']['bucket']['name']
            source_key = unquote_plus(record['s3']['object']['key'])
            
            event_name = record['eventName']
            file_extension = os.path.splitext(source_key)[1].lower()
            url = f'https://{source_bucket}.s3.{REGION}.amazonaws.com/{source_key}'
    
            # Skip processing if the source_key starts with 'resized/' to prevent recursion
            if source_key.startswith('resized/'):
                continue
    
            # Construct the destination key for the resized image
            destination_key = 'resized/' + source_key
            thumb_url = f'https://{source_bucket}.s3.{REGION}.amazonaws.com/{destination_key}'
    
            # Handle folder creation and deletion logic
            if source_key.endswith('/'):
                if 'ObjectCreated:Put' in event_name:
                    
                    # Folder creation logic
                    try:
                        s3.put_object(Bucket=source_bucket,  Key=destination_key)
                        folder = source_key.rsplit('/', 1)[0]
                        fid = get_id(folder, cursor)
                        result = source_key.rsplit('/', 2)[0]
                        parent_folder_id = get_id(result, cursor)
                        print(parent_folder_id)
                        data = (folder, parent_folder_id)
                        if fid is None and parent_folder_id is not None:
                             insert_folder(data, cursor)
                        batch(source_bucket, '2024-OLY/', cursor)
                    except Exception as e:
                        return {'statusCode': 500, 'body': f"Error creating folder at {destination_key}: {str(e)}"}
    
                elif 'ObjectRemoved:' in event_name:
                    # Folder deletion logic
                    try:
                        #delete_all_objects(source_bucket, destination_key)
                        s3resource.Bucket(source_bucket).objects.filter(Prefix=destination_key).delete()
                        s3.delete_object(Bucket=source_bucket, Key=destination_key)
                        #delete result folder in DB
                        result = source_key.rsplit('/', 1)[0]
                        folder_id = get_id(result, cursor)
                        delete_all_objects(folder_id)
                        delete_folder(result)
                        delete_child(source_key)
                    except Exception as e:
                        return {'statusCode': 500, 'body': f"Error deleting folder at {destination_key}: {str(e)}"}
            else:
                # Handle file-specific logic for image resizing and deletion
                result = source_key.rsplit('/', 1)[0]
                if 'ObjectRemoved:' in event_name:
                    # Image deletion logic
                    try:
    
                        s3.delete_object(Bucket=source_bucket, Key=destination_key)
                        delete_data(source_key)
                    except Exception as e:
                        return {'statusCode': 500, 'body': f"Error deleting resized image at {destination_key}: {str(e)}"}
                elif 'ObjectCreated:' in event_name and file_extension in ['.jpg', '.jpeg', '.png']:
                    # Folder creation logic
                    try:
                        # Download the image from S3
                        response = s3.get_object(Bucket=source_bucket, Key=source_key)
                        image_data = response['Body'].read()
                        image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_UNCHANGED)
    
                        # Resize the image using the provided resize logic
                        fixed_width = 500
                        original_height, original_width = image.shape[:2]
                        aspect_ratio = original_height / original_width
                        calculated_height = int(fixed_width * aspect_ratio)
                        resized_image = cv2.resize(image, (fixed_width, calculated_height))
    
                        if file_extension in ['.jpg', '.jpeg']:
                            s3.put_object(
                                Bucket=source_bucket,
                                Key=destination_key,
                                Body=cv2.imencode('.jpeg', resized_image)[1].tobytes(),
                                ContentType='image/jpeg'
                            )
                        if file_extension in ['.png']:
                            s3.put_object(
                                Bucket=source_bucket,
                                Key=destination_key,
                                Body=cv2.imencode('.png', resized_image)[1].tobytes(),
                                ContentType='image/png'
                            )       
                        batch(source_bucket, '2024-OLY/', cursor) 
                        fid = get_id(result, cursor)
                        meta = "[" + get_meta(source_bucket, source_key) + "]"
                        print(meta)
                        data = (url, thumb_url, '[{}]', source_key, fid, '')
                        insert_data(data, cursor)
                        update_meta(meta, source_key)
                    except Exception as e:
                        return {'statusCode': 500, 'body': f"Error creating folder at {destination_key}: {str(e)}"}
    
        # Return success status for folder operations

    source_key = unquote_plus(event['Records']['s3']['object']['key'])

    connection.close()
    if source_key.endswith('/'):

        return {'statusCode': 200, 'body': 'Folder operation processed successfully'}

    # Return success status for image operations
    if 'ObjectCreated:Put' in event_name and file_extension in ['.jpg', '.jpeg', '.png']:
        return {'statusCode': 200, 'body': 'Image operation processed successfully'}

    return {'statusCode': 400, 'body': 'Unhandled event type or path'}

def cleanup(event, context):
    global connection
    if connection:
        connection.close()

# Register the cleanup function to run when the Lambda execution environment is cleaned up
import atexit
atexit.register(cleanup)


 
