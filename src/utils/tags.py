# http://dicom.nema.org/dicom/2013/output/chtml/part05/sect_6.2.html
import datetime
from logger import get_logger
import pydicom
import os
import base64
import math

IGNORE_OB = os.getenv('IGNORE_OB', True)

log = get_logger(__name__)


def rep_string(elem):
    return serialize_sets(elem)


def convert_DA(elem):
    # Convert DICOM DA to ISO format for datatype DATE compability
    try:
        if not elem.is_empty:
            # Only return YYYY-MM-DD, exclude TIMESTAMP
            # date = datetime.datetime.strptime(elem.value, '%Y%m%d').date()
            date = rep_string(elem)
            if isinstance(date, list):
                date_list = []
                for item in date:
                    date_list.append(datetime.datetime.strptime(item, '%Y%m%d').date())
                return date_list
            return datetime.datetime.strptime(date, '%Y%m%d').date()
        return datetime.datetime.fromisoformat('1900-01-01').date()
    except Exception as e:
        log.error(e)
        raise


def convert_OB(elem):
    try:
        if elem.is_empty:
            return elem.value
        if IGNORE_OB:
            return 'IGNORED'
        else:
            return base64.standard_b64encode(elem.value)
    except Exception as e:
        log.error(e)
        raise


def convert_TM(elem):
    # return string, athena does not support TIME data type ex HHMMSS.FFFFFF
    return rep_string(elem)


def generate_PN(elem):
    return {
        'FamilyName': elem.family_name,
        'GivenName': elem.given_name,
        'Ideographic': elem.ideographic,
        'MiddleName': elem.middle_name,
        'NamePrefix': elem.name_prefix,
        'NameSuffix': elem.name_suffix,
        'Phonetic': elem.phonetic,
    }


def convert_PN(elem):
    try:
        if (elem.is_empty):

            return {

                'FamilyName': '',
                'GivenName': '',
                'Ideographic': '',
                'MiddleName': '',
                'NamePrefix': '',
                'NameSuffix': '',
                'Phonetic': '',
            }
        data = rep_string(elem)
        # if list of PN iterate to construct common data structure
        if isinstance(data, list):
            PN_LIST = []
            for item in data:
                PN_LIST.append(generate_PN(item))
            return PN_LIST
        else:
            return generate_PN(elem.value)
    except Exception as e:
        log.error(e)
        raise


def serialize_sets(obj):
    try:
        # pydicom.multival.MultiValue type into serialize list
        # if isinstance(obj.value, pydicom.multival.MultiValue):
        #     if len(obj.value._list) > 0:
        #         # return list(map(str, obj.value._list))
        #         return obj.value._list
        #     else:
        #         return ''
        # if obj.VM > 1:
        #     return obj.value
        # else:
        #     return str(obj.value)
        return validate_vm(obj)
    except Exception as e:
        log.error(e)
        raise


def validate_vm(obj):
    try:
        log.debug(f'Validing Tag {obj.keyword} VM : {obj.VM} VR: {obj.VR}')
        maxVM = pydicom._dicom_dict.DicomDictionary[obj.tag][1]
        split = maxVM.split('-')
        if len(split) > 1:
            min = split[0]
            max = split[1]
            if 'n' in max:
                max = math.inf
            else:
                max = int(max)
        else:
            min = int(split[0])
            max = min
        if max > 1:
            if isinstance(obj.value, pydicom.multival.MultiValue):
                return obj.value._list
            elif isinstance(obj.value, list):
                return obj.value
            else:
                return [obj.value]
        else:
            return str(obj.value)
    except Exception as e:
        log.error(e)
        raise


def convert_SQ(elem):
    try:
        sq = {}
        if (not elem.is_empty):
            # sq[elem.keyword] = {}
            for item in elem.value._list:
                # Check for empty pydicom dataset
                if item == pydicom.Dataset():
                    return None
                for i in item:
                    sq[i.keyword] = vr_select(i)(i)
            return sq
        return rep_string(elem)
    except Exception as e:
        log.error(e)
        raise


def return_integer(elem):
    try:
        if not elem.is_empty:
            return rep_string(elem)
        return int(0)
    except Exception as e:
        log.error(e)
        raise


def return_lo(elem):
    return elem.value


def return_float(elem):
    try:
        if not elem.is_empty:
            # if elem.VM == 1:
            #     return [float(elem.value)]
            # return rep_string(elem)
            return rep_string(elem)
        return float(0)
    except Exception as e:
        log.error(e)
        raise


def convert_DT(elem):
    try:
        if not elem.is_empty:
            # Only return YYYY-MM-DD, exclude TIMESTAMP
            # date = datetime.datetime.strptime(elem.value, '%Y%m%d').date()
            date = rep_string(elem)
            if isinstance(date, list):
                date_list = []
                for item in date:
                    date_list.append(datetime.datetime.strptime(item, '%Y%m%d%H%M%S.%f%z'))
                return date_list
            return datetime.datetime.strptime(elem.value, '%Y%m%d%H%M%S.%f%z')
        return datetime.datetime.strptime(elem.value, '%Y%m%d%H%M%S.%f%z')
    except Exception as e:
        log.error(e)
        raise


def vr_select(elem):
    return {
        'AE': rep_string,
        'AS': rep_string,
        'AT': return_integer,  # return integer
        'CS': rep_string,  # return string
        'DA': convert_DA,  # return datetime in YYYY-MM-DD format
        'DS': rep_string,
        'DT': convert_DT,  # return Timestamp
        'FD': rep_string,  # return float
        'FL': return_float,  # return float
        'IS': rep_string,
        'LO': rep_string,  # return string
        'LT': rep_string,
        'OB': rep_string,
        'OD': rep_string,
        'OF': rep_string,
        'OL': rep_string,
        'OW': rep_string,
        'OV': rep_string,
        'PN': convert_PN,  # return string if empty or return dict,
        'SH': rep_string,  # return string
        'SL': return_integer,
        'SQ': convert_SQ,  # return struct
        'SS': return_integer,
        'ST': rep_string,
        'SV': rep_string,
        'TM': convert_TM,  # return string, TIME data type is not supported.
        'UC': rep_string,
        'UI': rep_string,  # return string
        'UL': return_integer,  # return integer
        'UN': rep_string,
        'UR': rep_string,
        'US': return_integer,  # return integer
        'UT': rep_string,
        'UV': rep_string,
        'OB': rep_string,
        'OW': rep_string,
        'US': rep_string,
        'SS': rep_string,
        'US': rep_string,
        'OW': rep_string,
        'US': rep_string,
        'SS': rep_string,
        'OW': rep_string,
    }.get(elem.VR, f'Invalid VR {elem.VR} tag')
