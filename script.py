import json
import sys

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


def dicom_dataset_to_json(dicom_header):
    dicom_dict = {}
    repr(dicom_header)
    for dicom_value in dicom_header.values():
        if dicom_value.tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        if type(dicom_value.value) == pydicom.dataset.Dataset:
            dicom_dict[dicom_value.tag] = dicom_dataset_to_json(dicom_value.value)
        else:
            v = _convert_value(dicom_value.value)
            dicom_dict[dicom_value.tag] = v
    return json.dumps(dicom_dict)


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


if __name__ == '__main__':
    dicom_file = get_dicom_file()

    # Extrai o dataset do arquivo dcm
    dicom_dataset = pydicom.dcmread(dicom_file)

    # Busca o id do exame para inserir no HBase e no couchDb
    id_study = dicom_dataset.StudyInstanceUID

    connection = happybase.Connection("localhost")

    # Conecta a tabela dicom
    dicom_table = connection.table('dicom')

    # Converte o dataset dicom em json
    dicom_dataset_json = dicom_dataset_to_json(dicom_dataset)

    # Insere o exame
    dicom_table.put(id_study, {b'imagem:': dicom_dataset_json})
