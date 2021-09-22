import json
import urllib.parse
import boto3
import uuid
import re

s3_client = boto3.client('s3')
stepFunctions_client = boto3.client('stepfunctions')
project_name = 'reciprocal_rec_system'

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    input_filename = key[key.rfind('/')+1:]
    wf_status = None

    if re.match("input_20\d{2}\d{2}\d{2}",input_filename):
        copy_status = copy_files(bucket, key)
        wf_status = execute_workflow(bucket)
        
    else:
        # print("Input filename did not match pattern")
        wf_status = 'Workflow execution not started'
    
    return wf_status

def copy_files(bucket, input_source_key):
    
    copy_input_source = {
      'Bucket': bucket,
      'Key': input_source_key
    }
    
    input_dest_key = project_name + '/data/input/input.parquet.gzip'
    
    s3_client.copy(copy_input_source, bucket, input_dest_key)

    return 'Data copied successfully'


def execute_workflow(bucket):
    
    id = uuid.uuid4().hex

    
    state_machine_arn = 'arn:aws:states:ap-northeast-1:987654321:stateMachine:End2End-Routine-865a33537be64feb96147899936e61c5'
    state_machine_name = 'Workflow-'+ id
    
    
    processing_function_name = 'query-processing-status'
    create_preprocessing_function_name = 'create-preprocessing-job'
    create_batch_pred_function_name = 'create-batch-pred-job'
    glue_job_name = 'glue-batch-load-recs'
    endpoint_name = 'recommender-endpoint'
    
    
    inputs={
            'ProcessingLambdaFunctionName': processing_function_name,
            'CreatePreprocessingLambdaFunctionName': create_preprocessing_function_name,
            'CreateBatchPredLambdaFunctionName': create_batch_pred_function_name,        
            'PreprocessingJobName': 'user-transform-etl-{}'.format(id),
            'TrainingJobName': 'train-{}'.format(id),
            'ModelName': 'recommender-model-{}'.format(id),
            'EndpointName': endpoint_name,
            'BatchPredJobName': 'recommender-batch-transform-{}'.format(id),
            'GlueBatchJobName': glue_job_name,
            'S3ModelPath': 's3://{}/{}/data/model/train-{}/output'.format(bucket, project_name, id),
            'S3PreprocessedPath': 's3://{}/{}/data/train/preprocessed-{}'.format(bucket, project_name, id),
            'S3RecommendationsPath': 's3://{}/{}/data/output/recommendations-{}'.format(bucket, project_name, id),
            'DoPreprocessing':True,
            'DoTraining':True,
            'DoBatchRecommend':True,
            'CreateNewEndpoint':True
        }
    
    stepFunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=state_machine_name,
        input=json.dumps(inputs)
    )
    
    return 'Workflow execution started' 

