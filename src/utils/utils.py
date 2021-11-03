import os
from logger import get_logger
log = get_logger(__name__)


def unzip(zip_archive):
    log.debug(f'Prep to unzip {zip_archive.filename}')
    list_files = []
    for file in zip_archive.infolist():
        # Skip Directories and DICOMDIR file
        if not file.is_dir() and (file.filename.upper().find('DICOMDIR') == -1):
            # Check if DICOM header is present
            f = zip_archive.open(file)
            # f.read(128)
            # magic = f.read(4)
            # if magic != b'DICM':
            #     log.info(f'Ignore File in ZipFile, Not Valid DCM file "{file.filename}"')
            if check_dcm(f):
                # Add File to list
                list_files.append(zip_archive.open(file))
                log.debug(f'Added "{file.filename}" to process queue')
            else:
                log.info(
                    f'Ignore File in ZipFile, Not Valid DCM file "{file.filename}"')
            f.close()
        else:
            log.info(f'Ignore file-path in ZipFile "{file.filename}"')
    return list_files


def tar(tar_archive):
    log.debug(f'Prep to tar/bz2/gz {tar_archive.name}')
    list_files = []
    for file in tar_archive.getmembers():
        if file.isfile() and (file.name.upper().find('DICOMDIR') == -1):
            f = tar_archive.extractfile(file)
            if check_dcm(f):
                tarfile = tar_archive.extractfile(file)
                tarfile.tarname = file.name
                list_files.append(tarfile)
                log.debug(f'Added {file.name} to process queue')
            else:
                log.info(
                    f'Ignore File in TarFile, Not Valid DCM files "{file.name}"')
            f.close()
        else:
            log.info(f'Ignore file-path in TarFile "{file.name}"')
    return list_files


def check_dcm(file):
    # Skip DCM preamble in file
    file.read(128)
    header = file.read(4)
    if header != b'DICM':
        return False
    else:
        return True


def getname(name):
    if hasattr(name, 'tarname'):
        return name.tarname
    elif hasattr(name, 'name'):
        return name.name
    else:
        return os.path.split(name)[1]
