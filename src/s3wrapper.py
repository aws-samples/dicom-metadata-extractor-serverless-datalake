import os
import boto3
import tarfile
from logger import get_logger
import zipfile
import utils.utils as utils
IGNORE_FILE_EXT = ['.json', '.txt', '.csv']

log = get_logger(__name__)


class s3file():
    def __init__(self, s3bucket, s3key, s3region, size=0):
        self.s3_bucket = s3bucket
        self.s3_key = s3key
        self.s3_region = s3region
        self.size = self.set_size(size)
        self.local_dir = '/tmp'
        self.s3_client = self.generate_s3_client()
        self.file_ext = '.dcm'
        self.file_location = ''
        self.file_list = []

    def __repr__(self):
        return f'{self.s3_region} - s3://{self.s3_bucket}/{self.s3_key}'

    def set_size(self, size):
        try:
            return int(size)
        except Exception as e:
            log.error(e)
            raise

    def generate_s3_client(self):
        s3 = boto3.client('s3', region_name=self.s3_region)
        return s3

    def eval_ext(self):
        _, ext = os.path.splitext(self.s3_key)
        if ext in IGNORE_FILE_EXT:
            log.info(
                f'File ext {ext} in IGNORE list , skip process and return success {self}')

        self.set_file_ext(ext)

    def download_file(self):
        try:
            folderpath, _ = os.path.split(self.s3_key)
            os.makedirs(f'{self.local_dir}/{folderpath}', exist_ok=True)
            log.debug(f'Created folder {self.local_dir}/{folderpath}')
            _, ext = os.path.splitext(f'{self.local_dir}/{self.s3_key}')
            log.info(f'Downloading file {self}')
            if self.file_ext == '.dcm':
                log.debug(f'Found dcm file type download first 10 MBs of file')
                # Hard coded get only first 10MB of data or less
                start = 0
                end = 10000000
                resp = self.s3_client.get_object(
                    Bucket=self.s3_bucket, Key=self.s3_key, Range=f'bytes={start}-{end}')['Body'].read()
                with open(f'{self.local_dir}/{self.s3_key}', 'wb+') as f:
                    f.write(resp)
                log.debug(
                    f'Completed bytes written {end} bytes to local location')
            else:
                self.s3_client.download_file(
                    self.s3_bucket, self.s3_key, f'{self.local_dir}/{self.s3_key}')
            log.info(f'Completed download {self}')
            self.file_location = f'{self.local_dir}/{self.s3_key}'

        except Exception as e:
            log.error(
                f'Unable to download file s3://{self.s3_bucket}/{self.s3_key} in region {self.s3_region}')
            log.error(e)
            raise

    def set_file_ext(self, ext):
        if (ext != '' and len(ext) < 10):
            self.file_ext = ext.lower()
            log.debug(f'Set file extension to be {ext}')
        else:
            default_ext = os.environ.get('DEFAULT_S3_FILE_EXTENTSION', '.dcm')
            log.info(
                f'Unable to evaluate file ext:{ext}, continue assuming {default_ext}')
            self.file_ext = default_ext

    def get(self):
        log.debug(f'Selected file ext {self.file_ext}')
        if (self.file_ext in IGNORE_FILE_EXT):
            log.info(f'File ext: {self.file_ext} is IGNORED')
        elif (self.file_ext == '.dcm'):
            log.debug(
                f'Select .dcm file type for processing, return file location: {self.file_location}')
            self.download_file()
            self.file_list.append(self.file_location)
        elif (self.file_ext == '.zip'):
            self.download_file()
            if (zipfile.is_zipfile(self.file_location)):
                archive = zipfile.ZipFile(self.file_location, 'r')
                self.file_list = utils.unzip(archive)
            else:
                log.error(
                    f'Invalid ZipFile {self} downloaded to {self.file_location}')
                raise
        elif (self.file_ext == '.bz2'):
            log.info(f'Select .bz2 file extension, continue assuming tar.bz2')
            self.download_file()
            archive = tarfile.open(self.file_location, 'r')
            self.file_list = utils.tar(archive)
        elif (self.file_ext == '.tar'):
            log.info(
                f'Select .tar file type for processing {self.file_location}')
            self.download_file()
            archive = tarfile.open(self.file_location, 'r')
            self.file_list = utils.tar(archive)
        elif (self.file_ext == '.gz'):
            log.info(f'Select .gz file extension, continue assuming tar.gz')
            self.download_file()
            archive = tarfile.open(self.file_location, 'r')
            self.file_list = utils.tar(archive)
        else:
            log.error(f'Unexpected file extension {self.file_ext}')
            raise Exception(f'{self.file_ext} file extension not supported')
