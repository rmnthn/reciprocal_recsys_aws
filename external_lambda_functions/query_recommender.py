import json
import boto3
from boto3.dynamodb.conditions import Key
import time
sm_rt_client = boto3.client('runtime.sagemaker')
ssm_client = boto3.client('ssm')
db_client = boto3.resource('dynamodb')
# Connect to recommendations table in DDB
recommendations = db_client.Table('recommendations')
sm_endpoint_name = 'recommender-endpoint'

def lambda_handler(event, context):
    try:
        
        #print("Got event\n", event)
        
        # Process POST request for unregistered users
        if 'body' in event and event['body'] is not None:
            response_body, status_code = handle_new_user_query(event['body'])
        
        # Process GET request for registered users    
        else:
            query_user_id = event['queryStringParameters']['q_user_id']
            ddb_query_response = recommendations.query(
                    KeyConditionExpression=Key('user_id').eq(query_user_id)
            )
            query_model_id = -1
            if 'q_model_id' in event['queryStringParameters'].keys():
                query_model_id = int(event['queryStringParameters']['q_model_id'])
        
            # Check if user_id has got pre-computed recommendations
            # Otherwise, potential new user, ask for user attributes
            item_exists = check_if_item_exists(event, ddb_query_response['Items'])

            if item_exists:
                ddb_recs = ddb_query_response['Items'][query_model_id]
                
            # Requests are abstracted as queries based on model_id
            # model_id is a flexible indicator for any concept in future, depending on the use case.
            # It could be a set of recommendations from different models,
            # could be a set of recommendations from the same model derived from different preference clusters,
            # or simply a set of recommendations made at different time points.
            
    
            if event['queryStringParameters']['q_type'] == 'PAGINATED_QUERY':
                if not item_exists:
                    response_body, status_code = handle_potential_new_user(event)
                elif 'q_page_id' in event['queryStringParameters'].keys():
                    response_body, status_code = handle_paginated_query(event['queryStringParameters'], ddb_recs)
                else:
                    response_body, status_code = handle_malformed_query(event, ['page_id'])
                    
            elif event['queryStringParameters']['q_type'] == 'RANGE_QUERY':
                if not item_exists:
                    response_body, status_code = handle_potential_new_user(event)
                else:
                    response_body, status_code = handle_range_query(event['queryStringParameters'], ddb_recs)
    
            else:
                response_body, status_code = handle_malformed_query(event, ['query_type', 'user_id', 'page_id', 'model_id'])
        
        if 'expiry_time' in response_body:
            del response_body['expiry_time']
        if 'model_id' in response_body:
            response_body['model_id'] = str(response_body['model_id'])

        string_body  = json.dumps(response_body)
        response_object = {
            'statusCode': status_code,
            'headers': {'Content-Type': 'application/json'},
            'body': string_body,
        }  
    
    except Exception as e:
        print("Exception occured: ")
        print(e)
        
    else:
        return response_object
    
def handle_paginated_query(event, recommendations_resp):
    ''' Get paginated recommendations for a given (user_id, page_id) pair'''
    
    # Parse query string parameters
    query_page_id = int(event['q_page_id'])
    response_body = recommendations_resp
    
    # Current recommendations per page = 10
    recs_per_page = 10
    page_start = (query_page_id-1)*recs_per_page
    page_end = (query_page_id-1)*recs_per_page+recs_per_page
    response_body['recommendation_id'] = response_body['recommendation_id'].split(',')[page_start: page_end]
    response_body['message'] = 'Paginated response for the requested page_id.'
    status_code = 200
    return response_body, status_code
    
def handle_range_query(event, recommendations_resp):
    '''Get multiple sets of recommendations for a given user_id where the sets come from pages page_start and page_end specified in the query'''
    
    response_body = recommendations_resp
    # Parse query string parameters
    if 'q_page_id' in event:
        query_rec_id = event['q_page_id']
    
        rec_start, rec_end = query_rec_id.split('_')
        rec_start, rec_end = int(rec_start), int(rec_end)
        
        # Current recommendations per page = 100
        recs_per_page = 100
        page_start = (rec_start-1)*recs_per_page
        page_end = (rec_end-1)*recs_per_page+recs_per_page
        response_body['recommendation_id'] = response_body['recommendation_id'].split(',')[page_start: page_end]
    else:
        response_body['recommendation_id'] = response_body['recommendation_id'].split(',')
        
    response_body['message'] = 'Range query response. Returning the set of recommendations for the requested range.'
    status_code = 200
    return response_body, status_code
    
def handle_potential_new_user(event):
    response_body = "Potential new user. Send user's features as a separate query."
    status_code = 202
    return response_body, status_code

def handle_malformed_query(event, missing_parameters=[]):
    response_body = {'message': 'Malformed paginated query. Missing parameter(s):' + ','.join(missing_parameters) + '.'}
    status_code = 400
    return response_body, status_code

def check_if_item_exists(event, recommendations_resp):
    if len(recommendations_resp) <= 0:
        return False
    return True
    
def handle_new_user_query(event):
    event = json.loads(event)

    payload = {
               'user_id':event['new_user_id'],
               'gender':event['gender'],
               'locationId':event['locationId'],
               'birthdate':event['birthdate'],
               'followingCategories': event['followingCategories']
              }

    # Get recommendaitons for unregistered users from the latest model
    response = sm_rt_client.invoke_endpoint(EndpointName=sm_endpoint_name, Body=json.dumps(payload), ContentType='application/json')
    recommendations_new = json.loads(response['Body'].read().decode("utf-8"))
    
    
    # Write the recommendations to DynamoDB
    del recommendations_new['index']
    curr_time_ddb = int(time.time())
    retrain_freq_in_days = (ssm_client.get_parameter(Name='recommender.retrain_freq', WithDecryption=True))['Parameter']['Value']
    ttl_in_days = int(retrain_freq_in_days) * 2
    ttl_in_seconds = int(ttl_in_days)*24*3600 #14 days
    expiry_time_ddb = curr_time_ddb + ttl_in_seconds
    recommendations_new['model_id'] = curr_time_ddb
    recommendations_new['expiry_time'] = expiry_time_ddb

    ddb_put_response = recommendations.put_item(
        Item={
            'user_id': recommendations_new['user_id'],
            'model_id': recommendations_new['model_id'],
            'recommendation_id': recommendations_new['recommendation_id'],
            'expiry_time': recommendations_new['expiry_time']
        }
    )
    # print("PutItem succeeded: ", ddb_put_response)

    # Return response    
    event['q_model_id'] = '-1'
    if event['q_type'] == 'PAGINATED_QUERY':
        if 'q_page_id' in event.keys():
            response_body, status_code = handle_paginated_query(event, recommendations_new)
        else:
            response_body, status_code = handle_malformed_query(event, ['page_id'])
                
    elif event['q_type'] == 'RANGE_QUERY':
            response_body, status_code = handle_range_query(event, recommendations_new)

    response_body['message'] = '[Unregistered user] ' + response_body['message']

    return response_body, status_code
    
