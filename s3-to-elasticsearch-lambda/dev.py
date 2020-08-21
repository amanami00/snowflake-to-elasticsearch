import boto3
import requests
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError
import json

success_folder = 'sf-success'
error_folder = 'sf-error'
index = '{ "index" : {} }\n'

region = 'us-east-1'
service = 'es'

credentials = boto3.Session().get_credentials()
s3_client = boto3.client('s3')

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


def lambda_handler(event, context):
    event_type = event['Records'][0]['eventName']
    if event_type in ['ObjectCreated:Put']:
        file_name = event['Records'][0]['s3']['object']['key']
        file_bucket = event['Records'][0]['s3']['bucket']['name']
        file_obj = s3_client.get_object(Bucket=file_bucket, Key=file_name)
        json_data = file_obj['Body'].read()
        data = json_data.splitlines()
        finalData = ''
        

        for line in data:
            finalData += index
            finalData += line.decode("utf-8") + '\n'

        try:
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            response = requests.post(
                '<Elasticsearch Endpointhttps>/<index>/_bulk',
                auth=awsauth,
                headers=headers,
                data=finalData
            )
            responseData = json.loads(response.content.decode())
            if ('errors' in responseData and responseData['errors'] == False):
                print('Data has been successfully moved to successfully')
                dest_folder = success_folder
            else:
                print('Data has not been successfully moved to successfully')
                dest_folder = error_folder

            new_file_name = file_name.split('/')[1]
            source = f"{file_bucket}/{file_name}"
            destination = f"{dest_folder}/{new_file_name}"

            move_file(file_bucket, destination, source, file_name)
            print(f'File has been successfully moved to {dest_folder} folder')
        except requests.exceptions.RequestException as e:
            print(e.message)
            exit(1)

    return {
        "status": 200,
        "message": "Hello lambda"
    }

def move_file(file_bucket, destination, source, file_name):
    try:
        # Copy file into othre folder
        response = s3_client.copy_object(CopySource=source, Bucket=file_bucket, Key=destination)
        if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
            # Delete the file from source folder
            s3_client.delete_object(Bucket=file_bucket, Key=file_name)
    except ClientError as e:
        print(e.message)

