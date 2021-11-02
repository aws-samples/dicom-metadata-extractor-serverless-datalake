import logging
import pydicom
import boto3
import json
from utils.utils import getname
import os
from logger import get_logger
from s3wrapper import s3file
from dicomwrapper import dcmfile
import awswrangler as wr
import pandas as pd
import re
from urllib import parse

S3_BUCKET = os.environ.get('S3_BUCKET', None)
S3_KEY = os.environ.get('S3_KEY', None)
S3_REGION = os.environ.get('S3_REGION', None)
OBJ_SIZE = os.environ.get('OBJ_SIZE', None)
LOCAL_LOCATION = os.environ.get('LOCAL_LOCATION', '/tmp')
S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET', None)
S3_OUTPUT_BUCKET_REGION = os.environ.get('S3_OUTPUT_BUCKET_REGION', 'us-east-1')
GLUE_DATABASE_NAME = os.environ.get('GLUE_DATABASE_NAME', 'dicom')
GLUE_TABLE_NAME = os.environ.get('GLUE_TABLE_NAME', 'dicom_metadata')
MAX_LAMBDA_SIZE = os.environ.get('MAX_LAMBDA_SIZE', 500)
AWS_BATCH_QUEUE = os.environ.get('AWS_BATCH_QUEUE', 'dicom-queue')
AWS_BATCH_DEFINITION = os.environ.get('AWS_BATCH_DEFINITION', 'dicom-parser')
PARTITION_COL = os.environ.get('PARTITION_COL', 'study_date')
log = get_logger(__name__)


def transform(name, img, dcm):
    log.info(f'Flatten {name} structure')
    dcm.append(name, img)


def output(dcm):
    log.debug(f'Convert data structure to dataframe')
    df = pd.DataFrame.from_dict(dcm.img_list)
    # Drop columns with all NONE Values
    df.dropna(axis=1, how='all', inplace=True)
    session = boto3.session.Session(region_name=S3_OUTPUT_BUCKET_REGION)
    try:
        parquet = wr.s3.to_parquet(
            df=df,
            boto3_session=session,
            path=f's3://{S3_OUTPUT_BUCKET}/',
            compression='snappy',
            dataset=True,
            sanitize_columns=True,
            s3_additional_kwargs={
                'ServerSideEncryption': 'AES256',
                'Tagging': parse.urlencode({"S3_BUCKET": dcm.source_s3_bucket, "S3_KEY": dcm.source_s3_key})
            },
            partition_cols={
                PARTITION_COL,
            },
        )
        log.info(f'Completed output, {parquet}')
        return parquet
    except Exception as e:
        log.error(f'Unable to convert df to parquet')
        log.error(e)
        raise


def inspect(dcm, ds):
    ds.get()
    if len(ds.file_list) > 0:
        try:
            for img in ds.file_list:
                name = getname(img)
                log.info(f'Processing {ds} - {name}')
                image = pydicom.dcmread(fp=img, stop_before_pixels=True)
                image.remove_private_tags()
                if hasattr(img, 'close'):
                    img.close()
                transform(name, image, dcm)
            log.info(f'Completed Dicom Parsing {dcm}, found {dcm.size} files')
            return output(dcm)
        except pydicom.errors.InvalidDicomError as i:
            log.error(f'Invalid Dicom file')
            log.error(i)
            raise
        except Exception as e:
            log.error(e)
            raise
    else:
        return{
            "paths": f'No file found, file ext: {ds.file_ext}'
        }

# Start AWS Lambda Function


def lambda_handler(event, context):

    log.debug('Running in Lambda Function')
    log.debug(json.dumps(event))
    S3_BUCKET = event['Records'][0]['s3']['bucket']['name']
    S3_KEY = event['Records'][0]['s3']['object']['key']
    S3_REGION = event['Records'][0]['awsRegion']
    OBJ_SIZE = event['Records'][0]['s3']['object']['size']
    dcm = dcmfile(source_s3_bucket=S3_BUCKET, source_s3_bucket_region=S3_REGION, source_s3_key=S3_KEY, source_s3_size=OBJ_SIZE)
    ds = s3file(s3bucket=dcm.source_s3_bucket, s3key=dcm.source_s3_key, s3region=dcm.source_s3_bucket_region, size=dcm.source_s3_size)
    ds.eval_ext()
    if (S3_BUCKET is None or S3_KEY is None or S3_REGION is None or OBJ_SIZE is None):
        log.error(f'Empty S3 input values; S3_BUCKET={S3_BUCKET}, \
            S3_KEY={S3_KEY}, S3_REGION={S3_REGION}')
        raise ValueError
    else:
        log.info(f'S3 input values; S3_BUCKET={S3_BUCKET}, S3_KEY={S3_KEY}, S3_REGION={S3_REGION} FileSize={OBJ_SIZE}')
    # DCM files can be processed on Lambda by downloading the first 10 MB to process only
    if OBJ_SIZE > (MAX_LAMBDA_SIZE * 1024 * 1024) and ds.file_ext != '.dcm':
        job_name = re.sub(r'\W+', '', S3_KEY[:128])
        log.info(f'Filesize greater than {MAX_LAMBDA_SIZE}MB, submit to AWS BATCH Queue: {AWS_BATCH_QUEUE} JobName: {job_name}')
        try:
            batch = boto3.client('batch')
            result = batch.submit_job(
                jobName=job_name,
                jobQueue=AWS_BATCH_QUEUE,
                jobDefinition=AWS_BATCH_DEFINITION,
                containerOverrides={
                    'environment': [
                        {
                            'name': 'S3_BUCKET',
                            'value': S3_BUCKET
                        },
                        {
                            'name': 'S3_KEY',
                            'value': S3_KEY
                        },
                        {
                            'name': 'OBJ_SIZE',
                            'value': str(OBJ_SIZE)
                        },
                        {
                            'name': 'S3_REGION',
                            'value': S3_REGION
                        },
                        {
                            'name': 'GLUE_TABLE_NAME',
                            'value': GLUE_TABLE_NAME
                        },
                        {
                            'name': 'GLUE_DATABASE_NAME',
                            'value': GLUE_DATABASE_NAME
                        },
                        {
                            'name': 'S3_OUTPUT_BUCKET',
                            'value': S3_OUTPUT_BUCKET
                        },
                        {
                            'name': 'S3_OUTPUT_BUCKET_REGION',
                            'value': S3_OUTPUT_BUCKET_REGION
                        },

                        {
                            'name': 'LOGLEVEL',
                            'value': logging.getLevelName(log.level)
                        },
                        {
                            'name': 'PARTITION_COL',
                            'value': PARTITION_COL
                        },

                    ]
                }

            )
            log.info(f'Forwarded request to AWS Batch {dcm}, JOB_ARN: {result["jobArn"]}')
            return {
                'code': 200,
                'message': f'Forwarded request to AWS Batch {dcm}, JOB_ARN: {result["jobArn"]}'
            }
        except Exception as e:
            log.error(e)
            raise
    output_location = inspect(dcm, ds)
    return {
        'code': 200,
        'message': f'Completed job INPUT {dcm}, OUTPUT {output_location["paths"]}'
    }


# Start AWS Batch
if __name__ == '__main__':
    log.debug(f'Start Print ENV Variables')
    for item, value in os.environ.items():
        log.debug(f'{item}={value}')
    log.debug(f'End Print ENV Variables')
    if (S3_BUCKET is None or S3_KEY is None or S3_REGION is None or OBJ_SIZE is None):
        log.error(f'Empty S3 input values; S3_BUCKET={S3_BUCKET}, \
            S3_KEY={S3_KEY}, S3_REGION={S3_REGION} OBJ_SIZE={OBJ_SIZE}')
        raise ValueError
    else:
        log.info(f'S3 input values; S3_BUCKET={S3_BUCKET}, S3_KEY={S3_KEY}, S3_REGION={S3_REGION} OBJ_SIZE={OBJ_SIZE}')
    dcm = dcmfile(source_s3_bucket=S3_BUCKET, source_s3_bucket_region=S3_REGION, source_s3_key=S3_KEY, source_s3_size=OBJ_SIZE)
    ds = s3file(s3bucket=dcm.source_s3_bucket, s3key=dcm.source_s3_key, s3region=dcm.source_s3_bucket_region, size=dcm.source_s3_size)
    ds.eval_ext()
    output_location = inspect(dcm, ds)
    log.info(f'Completed job INPUT s3://{S3_REGION}/{S3_BUCKET}/{S3_KEY}, OUTPUT {output_location["paths"]}')
