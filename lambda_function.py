import json
import logging
import boto3
import csv
logger = logging.getLogger()
logger.setLevel(logging.INFO)

destination_s3_bucket = 'devopslatam02-result-kc'
destination_folder = 'publications'

athena_staging_table = 'publications_staging'
athena_staging_database = 'devopslatam02'
athena_s3_results_folder = 'athena_results'
query = 'SELECT * FROM "workshoplabkc"."publication" limit 10;'
DATABASE = 'workshoplabkc'
output='s3://devopslatam02-result-kc/athena_results/'
def lambda_handler(event, context):
    logger.info(f"event {event}")
    logger.info(f"context {context}")
    
   #uploaded_file_key = event["Records"][0]['s3']['object']['key']
    
    # publications_20220501000000.csv
    #uploaded_file_name = uploaded_file_key[ uploaded_file_key.find('/') + 1: ]
    #logger.info(f"uploaded_file_name {uploaded_file_name}")
    
    client = boto3.client('athena')

    # Execution
    #response = client.start_query_execution(
    #    QueryString=query,
    #    QueryExecutionContext={
    #        'Database': DATABASE
    #    },
    #    ResultConfiguration={
    #        'OutputLocation': output,
    #    }
    #)
    s3_client = boto3.resource('s3')
    bucket = 'devopslatam02-result-kc'
    key = 'athena_results/Unsaved/2022/06/29/13bc6253-c000-4098-96ce-007b1c515b10.csv'
    s3_obj = s3_client.Object(bucket, key.replace('+', ' '))
    logger.info(f"s3_obj HERE {s3_obj}")
    response = client.get_query_results(
    QueryExecutionId='fa17903035b6d5b84f8e79d471e021fe',
    MaxResults=10
    )
    #data = s3_obj.get()['Body'].read()
    #data_csv = csv.DictReader(data.decode('utf-8').split('\n'), delimiter=',')
    #rows = [l for l in data_csv]
    return response