import base64
import json
import os
import sys

import couchdb
import happybase
import pydicom


def get_dicom_file(file):

    if not file:
        print('File {} not found'.format(file))

    return file


def __extract_dataset_to_dict(dicom_dict, dicom_dataset, file_data):

    if not dicom_dataset.SeriesInstanceUID in dicom_dict:
        dicom_dict[dicom_dataset.SeriesInstanceUID] = []

    data_instance = dict({'sopInstanceUid': dicom_dataset.SOPInstanceUID, 'data': file_data})
    dicom_dict.get(dicom_dataset.SeriesInstanceUID).append(data_instance)

    return dicom_dict[dicom_dataset.SeriesInstanceUID]


def _sanitise_unicode(s):
    return s.replace(u"\u0000", "").strip()


def _convert_value(v):
    t = type(v)
    if t in (list, int, float):
        cv = v
    elif t == str:
        cv = _sanitise_unicode(v)
    elif t == bytes:
        s = v.decode('ascii', 'replace')
        cv = _sanitise_unicode(s)
    elif t == pydicom.valuerep.DSfloat:
        cv = float(v)
    elif t == pydicom.valuerep.IS:
        cv = int(v)
    elif t == pydicom.valuerep.PersonName3:
        cv = str(v)
    else:
        cv = repr(v)
    return cv


def insert_in_hbase(rowkey, column_family, data):
    connection = happybase.Connection("localhost")

    dicom_table = connection.table('dicom')

    dicom_table.put(rowkey, {column_family: data})

    connection.close()


def insert_couchDb(dicom_dataset):
    # Insere no couchdb
    couch = couchdb.Server()

    dicom_db = 'dicom'

    db = couch[dicom_db]

    if not db:
        db = couch.create(dicom_db)

    doc = {'id': "",
           'idPaciente': dicom_dataset.PatientID,
           'nomePaciente': dicom_dataset.PatientName,
           'exames': []}

    db.save(doc)


def __validate_dir(path):
    directory_does_not_exist_exception = Exception("Directory does not exists. Please type a valid path")

    if not os.path.isdir(path):
        raise directory_does_not_exist_exception

    return path


def __define_column_family(dicom_dataset):

    if not dicom_dataset:
        return

    rowkey = dicom_dataset.StudyInstanceUID
    column_family = 'series:{}'.format(dicom_dataset.SeriesInstanceUID)

    return rowkey, column_family


if __name__ == '__main__':

    root = __validate_dir(sys.argv[1])

    print("Root dir:", root)

    dicom_dict = dict({})

    for path, subdirs, files in os.walk(root):
        for name in files:
            file = os.path.join(path, name)
            print('Working in file:', file)

            dicom_file = get_dicom_file(file)
            dicom_dataset = pydicom.dcmread(dicom_file)

            #compressed_file = base64.b16encode(dicom_dataset.pixel_array)
            compressed_file = "teste"

            dicom_dataset_dict = __extract_dataset_to_dict(dicom_dict, dicom_dataset, compressed_file)

            rowkey, column_family = __define_column_family(dicom_dataset)

            insert_in_hbase(rowkey, column_family, json.dumps(dicom_dataset_dict))

            #insert_couchDb(dicom_dataset)







