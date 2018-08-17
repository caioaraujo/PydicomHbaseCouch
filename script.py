import json
import sys

import couchdb
import pydicom
import happybase


def get_dicom_file():
    file_not_found_exception = Exception("Por favor, entre com uma imagem dicom")

    try:
        dicom_file = sys.argv[1]

        if not dicom_file:
            raise file_not_found_exception
    except IndexError:
        raise file_not_found_exception

    return dicom_file


def dicom_dataset_to_dict(dicom_header):
    dicom_dict = {}
    repr(dicom_header)
    for dicom_value in dicom_header.values():
        if dicom_value.tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        if type(dicom_value.value) == pydicom.dataset.Dataset:
            dicom_dict[dicom_value.tag] = dicom_dataset_to_dict(dicom_value.value)
        else:
            v = _convert_value(dicom_value.value)
            dicom_dict[dicom_value.tag] = v
    return dicom_dict


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


def insert_in_hbase(dicom_dataset):
    connection = happybase.Connection("localhost")

    dicom_table = connection.table('dicom')

    rowkey = dicom_dataset.StudyInstanceUID

    dicom_dict = dicom_dataset_to_dict(dicom_dataset)
    dicom_dataset_json = json.dumps(dicom_dict)


    dicom_table.put(rowkey, {b'serie:serieInstanceUid': dicom_dataset.StudyInstanceUID,
                             b'serie:sopInstanceUid': dicom_dataset.SOPInstanceUID,
                             b'serie:content': dicom_dataset_json})
    connection.close()


def insert_couchDb(dicom_dataset):
    # Insere no couchdb
    couch = couchdb.Server()

    dicom_db = 'dicom'

    db = couch[dicom_db]

    if not db:
        db = couch.create(dicom_db)

    doc = {'id': id_image,
           'idPaciente': dicom_dataset.PatientID,
           'nomePaciente': dicom_dataset.PatientName,
           'exames': []}

    db.save(doc)


if __name__ == '__main__':
    dicom_file = get_dicom_file()

    # Extrai o dataset do arquivo dcm
    dicom_dataset = pydicom.dcmread(dicom_file)

    # Recupera o dataset como dict
    #dicom_dataset_dict = dicom_dataset_to_dict(dicom_dataset)

    # Insere no hbase
    insert_in_hbase(dicom_dataset)

    #insert_couchDb(dicom_dataset)







