import boto3
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sm_client = boto3.client('sagemaker')

#Retrieve processsing job name from event and return processing job status
def lambda_handler(event, context):

    if ('ProcessingJobName' in event):
        job_name = event['ProcessingJobName']

    else:
        raise KeyError('ProcessingJobName key not found in function input!'+
                      ' The input received was: {}.'.format(json.dumps(event)))

    #Query boto3 API to check processing status.
    try:
        response = sm_client.describe_processing_job(ProcessingJobName=job_name)
        logger.info("Processing job:{} has status:{}.".format(job_name,
            response['ProcessingJobStatus']))
        print(response)

    except Exception as e:
        response = ('Failed to read processing status!'+ 
                    ' The processing job may not exist or the job name may be incorrect.'+ 
                    ' Check SageMaker to confirm the job name.')
        print(e)
        print('{} Attempted to read job name: {}.'.format(response, job_name))

    return {
        'statusCode': 200,
        'ProcessingJobStatus': response['ProcessingJobStatus']
    }