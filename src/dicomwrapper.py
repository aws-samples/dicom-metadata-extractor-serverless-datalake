from logger import get_logger
import os
import datetime
import utils.utils as utils
import utils.tags as tags

PARTITION_COL = os.environ.get('PARTITION_COL', 'study_date')

log = get_logger(__name__)


class dcmfile():
    def __init__(self, source_s3_bucket=None, source_s3_bucket_region=None, source_s3_key=None, source_s3_size=0):
        self.img_list = []
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_bucket_region = source_s3_bucket_region
        self.source_s3_key = source_s3_key
        self.source_s3_size = source_s3_size

    @property
    def size(self):
        return len(self.img_list)

    def __repr__(self):
        return f's3://{self.source_s3_bucket_region}/{self.source_s3_bucket}/{self.source_s3_key}'

    def update_s3_all(self, source_s3_bucket=None, source_s3_bucket_region=None, source_s3_key=None):
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_bucket_region = source_s3_bucket_region
        self.source_s3_key = source_s3_key

    def append(self, name, img):
        flat = self.transform(name, img)
        self.img_list.append(flat)

    def transform(self, name, img):
        # Full list of keywords https://github.com/pydicom/pydicom/blob/master/pydicom/_dicom_dict.py
        log.debug(f'Flattening {name} to dataset')
        element = {}
        for elem in img:
            try:
                if elem.keyword and not elem.is_empty:
                    elem_val = self.eval_vr_value(elem)
                    element[elem.keyword] = elem_val
                    log.debug(f'Adding tag {elem.keyword}: "{elem_val}" VR: {elem.VR}')
                else:
                    log.info(f'Ignore Tag: {elem.tag} VR: {elem.VR}')
            except Exception as e:
                log.error(f'Unable to process {name}, invalid tag keyword')
                log.error(f'Dump Invalid Elem tag: "{elem.tag}" VR: "{elem.VR}" name: "{elem.name}" keyword: "{elem.keyword}" value: {elem.repval}')
                log.error(e)
                raise
        element['SOURCE_S3_BUCKET'] = self.source_s3_bucket
        element['SOURCE_S3_REGION'] = self.source_s3_bucket_region
        element['SOURCE_S3_KEY'] = self.source_s3_key
        element['SOURCE_S3_ARCHIVE_PATH'] = name
        # Convert snake_case to CamelCase before checking
        cc_PARITION_COL = self.convert_cc(PARTITION_COL)

        if cc_PARITION_COL not in element:
            log.info(f'Missing {PARTITION_COL} adding value')
            element[cc_PARITION_COL] = datetime.datetime.fromisoformat('1979-01-01').date()

        return element

    def eval_vr_value(self, elem):
        return tags.vr_select(elem)(elem)

    def convert_cc(self, name):
        cc_convert = ''
        for x in name.split("_"):
            cc_convert += (x.capitalize())
        return cc_convert
