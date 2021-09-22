import boto3
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sm_client = boto3.client('sagemaker')

BASE_PROCESSING_IMAGE = ''
INPUT_DATA_PATH = '/opt/ml/processing/input_data'

PROCESSED_DATA_PATH = '/opt/ml/processing/processed_data'
DEFAULT_VOLUME_SIZE = 100
DEFAULT_INSTANCE_TYPE = 'ml.m5.12xlarge'
PREPROCESSING_CODE = '/opt/ml/code/preprocssesing_script.py'


def lambda_handler(event, context):
    """
    Creates a SageMaker Processing Job
    :param event:
    :param context:
    :return:
    """
    configuration = event['Configuration']
    print(configuration)
    try:
        response = sm_client.create_processing_job(
            ProcessingInputs=[
                {
                    'InputName':'input_data',
                    'S3Input':{
                        'S3Uri': configuration['S3InputDataPath'],
                        'LocalPath': INPUT_DATA_PATH,
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                    }
                }
            ],
            ProcessingOutputConfig={
                'Outputs': [
                    {
                        'OutputName':'processed',
                        'S3Output':{
                            'LocalPath': PROCESSED_DATA_PATH,
                            'S3Uri': configuration['S3OutputDataPath'],
                            'S3UploadMode': 'EndOfJob'
                         }
                     }
                 ]
            },
            ProcessingJobName=configuration['JobName'],
            ProcessingResources={
                'ClusterConfig':{
                    'InstanceCount': configuration.get('InstanceCount', DEFAULT_INSTANCE_COUNT),
                    'InstanceType': configuration.get('InstanceType', DEFAULT_INSTANCE_TYPE),
                    'VolumeSizeInGB': configuration.get('LocalStorageSizeGB', DEFAULT_VOLUME_SIZE)
                }
            },
            AppSpecification={
                'ImageUri': configuration.get('EcrContainerUri', BASE_PROCESSING_IMAGE),
                'ContainerEntrypoint': [
                    'python3', PREPROCESSING_CODE
                ],
            },
            RoleArn=configuration['IAMRole'],
        )
        return {
            'JobName': configuration['JobName']
        }
    except Exception as e:
        print("Error occurred creating SageMaker Preprocessing Job: {}".format(configuration['JobName']))
        print(e)
        raise
