# Produced by pydicom codify utility script
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.sequence import Sequence
from datetime import datetime
from pydicom.uid import generate_uid
from PIL import Image, ImageColor
from dateutil.relativedelta import relativedelta
import random


print('Generate single sample file')
sopclassinstanceuid = generate_uid()
color = random.choice(list(ImageColor.colormap.keys()))
print(f'Generate DCM with {color} for pixels')
file = f'sample_dcm/example-{random.randint(0,100)}'
list_dates = [datetime.today().strftime('%Y%m%d'), '19990101', '19870403']
# File meta info data elements
file_meta = FileMetaDataset()
file_meta.FileMetaInformationGroupLength = 242
file_meta.FileMetaInformationVersion = b'\x00\x01'
file_meta.MediaStorageSOPClassUID = '1.2.840.10008.5.1.4.1.1.7'
file_meta.MediaStorageSOPInstanceUID = sopclassinstanceuid
file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.1'
file_meta.ImplementationClassUID = '1.2.826.0.1.3680043.8.498.27364069006046809016231924679252811609'
file_meta.ImplementationVersionName = 'PYDICOM 1.4.2'

# Main data elements
ds = Dataset()
ds.SpecificCharacterSet = 'ISO_IR 192'
ds.ImageType = ['ORIGINAL', 'PRIMARY']
ds.SOPClassUID = '1.2.840.10008.5.1.4.1.1.7'
ds.SOPInstanceUID = sopclassinstanceuid
ds.StudyDate = random.choice(list_dates)
ds.ContentDate = ''
ds.AcquisitionDateTime = ''
ds.StudyTime = '120000'
ds.ContentTime = ''
ds.AccessionNumber = ''
ds.Modality = 'OT'
ds.ConversionType = 'SYN'
ds.ReferringPhysicianName = 'EMPTY'
ds.PatientName = 'EMPTY'
ds.PatientID = 'ID1'
ds.PatientSex = random.choice(['M', 'F'])
ds.PatientAge = f'0{random.randint(1,99)}Y'
ds.PatientBirthDate = (datetime.now() - relativedelta(years=int(ds.PatientAge.split('Y')
                       [0]), months=random.randint(1, 10), days=random.randint(0, 30))).date().strftime('%Y%m%d')
ds.PatientPosition = ''
ds.StudyInstanceUID = generate_uid()
ds.SeriesInstanceUID = generate_uid()
ds.StudyID = '1'
ds.SeriesNumber = '2'
ds.InstanceNumber = '1'
ds.PatientOrientation = ''
ds.Laterality = ''
ds.ImageComments = f'DCM with color {color}'
ds.SamplesPerPixel = 3
ds.PhotometricInterpretation = 'RGB'
ds.PlanarConfiguration = 0
ds.Rows = 979
ds.Columns = 985
ds.PixelSpacing = [1.0, 1.0]
ds.BitsAllocated = 8
ds.BitsStored = 8
ds.HighBit = 7
ds.PixelRepresentation = 0
ds.SmallestImagePixelValue = 0
ds.LargestImagePixelValue = 255
im = Image.new('RGB', (600, 600), ImageColor.getrgb(color))
ds.PixelData = im.tobytes()
ds.file_meta = file_meta
ds.is_implicit_VR = False
ds.is_little_endian = True
ds.save_as(file, write_like_original=False)
print(f'Saved DCM to {file}')
