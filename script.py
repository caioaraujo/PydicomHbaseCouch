import base64
import json
import os
import sys

import couchdb
import happybase
import pydicom
from thriftpy.transport import TTransportException


def get_dicom_file(file):

    if not file:
        print('File {} not found'.format(file))

    return file


def __extract_dataset_to_dict(dicom_dict, dicom_dataset, file_data):

    if dicom_dataset.SeriesInstanceUID not in dicom_dict:
        dicom_dict[dicom_dataset.SeriesInstanceUID] = []

    data_instance = dict({'sop_instance_uid': dicom_dataset.SOPInstanceUID, 'data': file_data})
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


def __create_dicom_table_in_hbase():

    try:
        connection = happybase.Connection("localhost")
    except TTransportException as e:
        raise Exception(f'Could not connect to hbase via thrift: {e.message}')

    connection.open()

    if b'dicom' in connection.tables():
        connection.close()
        return

    print('Creating dicom table in hbase')

    connection.create_table(
        'dicom',
        {'series': dict()}
    )

    connection.close()


def insert_in_hbase(rowkey, column_family, data):

    try:
        connection = happybase.Connection("localhost")
    except TTransportException as e:
        raise Exception(f'Could not connect to thrift: {e.message}')

    connection.open()

    dicom_table = connection.table('dicom')

    dicom_table.put(rowkey, {column_family: data})

    connection.close()


def insert_in_couchdb(key, dicom_dataset):
    couch = couchdb.Server()

    dicom_db = 'dicom'

    try:
        db = couch[dicom_db]
    except couchdb.ResourceNotFound:
        print("Nao encontrou o banco no couchdb. Tentando criar")
        db = couch.create(dicom_db)

    # Estrutura 1 - Documento do paciente com seus exames

    id_paciente = dicom_dataset.PatientID
    nome_paciente = str(dicom_dataset.PatientName)

    id_exame = dicom_dataset.StudyInstanceUID
    data_exame = dicom_dataset.StudyDate
    data_ano_exame = data_exame[0:4]
    dados_exame = {'id': id_exame, 'data': data_exame, 'ano': data_ano_exame}

    # busca o documento pelo id do paciente
    documento_paciente = db.get(id_paciente)

    if documento_paciente:
        exames = documento_paciente['exames']

        # Verifica se ja existe o exame no documento do paciente
        exame = list(filter(lambda x: x['id'] == id_exame, exames))

        if not exame:

            # Se o exame ainda nao ta cadastrado para o paciente, cria com as informacoes iniciais
            dados_exame['series'] = []
            exames.append(dados_exame)
        else:
            exame = exame[0]

            series = exame['series']

            id_serie = dicom_dataset.SeriesInstanceUID

            # Verifica se a serie ja existe no exame
            serie = list(filter(lambda x: x['id'] == id_serie, series))

            if not serie:
                desc_serie = dicom_dataset.SeriesDescription
                series.append({'id': id_serie, 'descricaoSerie': desc_serie})
            else:
                serie = serie[0]

                imagens = serie['imagens']

                id_imagem = dicom_dataset.SOPInstanceUID

                # Verifica se existe a imagem dentro da serie
                imagem = list(filter(lambda x: x['id'] == id_imagem, imagens))

                if not imagem:

                    peak_voltage = None
                    if hasattr(dicom_dataset, 'KVP'):
                        peak_voltage = dicom_dataset.KVP

                    exposure_time = None
                    if hasattr(dicom_dataset, 'ExposureTime'):
                        exposure_time = dicom_dataset.ExposureTime

                    tube_current = None
                    if hasattr(dicom_dataset, 'XRayTubeCurrent'):
                        tube_current = dicom_dataset.XRayTubeCurrent

                    relative_xray_exposure = None
                    if hasattr(dicom_dataset, 'RelativeXRayExposure'):
                        relative_xray_exposure = dicom_dataset.RelativeXRayExposure

                    ct_exposure_sequence_group = None
                    if hasattr(dicom_dataset, 'ExposureSequence'):
                        ct_exposure_sequence_group = dicom_dataset.ExposureSequence

                    ct_dose_index = None
                    if hasattr(dicom_dataset, 'ExposureIndex'):
                        ct_dose_index = dicom_dataset.ExposureIndex

                    imagem = {'id': id_imagem, 'peakVoltage': peak_voltage, 'exposureTime': exposure_time,
                              'tubeCurrent': tube_current, 'relativeXrayExposure': relative_xray_exposure,
                              'ctExposureSequenceGroup': ct_exposure_sequence_group, 'ctDoseIndex': ct_dose_index}

                    imagens.append(imagem)

    else:
        # Cria um novo documento para o paciente
        documento_paciente = {'_id': id_paciente,  # Id do paciente como chave do documento
                              'tipoDocumento': "EXAMES_PACIENTE",
                              'nomePaciente': nome_paciente,
                              'exames': [dados_exame]}

    db.save(documento_paciente)

    # Estrutura 2 - Documento com dados do exame

    # peak_voltage = None
    # if hasattr(dicom_dataset, 'KVP'):
    #     peak_voltage = dicom_dataset.KVP
    #
    # exposure_time = None
    # if hasattr(dicom_dataset, 'ExposureTime'):
    #     exposure_time = dicom_dataset.ExposureTime
    #
    # tube_current = None
    # if hasattr(dicom_dataset, 'XRayTubeCurrent'):
    #     tube_current = dicom_dataset.XRayTubeCurrent
    #
    # relative_xray_exposure = None
    # if hasattr(dicom_dataset, 'RelativeXRayExposure'):
    #     relative_xray_exposure = dicom_dataset.RelativeXRayExposure
    #
    # ct_exposure_sequence_group = None
    # if hasattr(dicom_dataset, 'ExposureSequence'):
    #     ct_exposure_sequence_group = dicom_dataset.ExposureSequence
    #
    # ct_dose_index = None
    # if hasattr(dicom_dataset, 'ExposureIndex'):
    #     ct_dose_index = dicom_dataset.ExposureIndex
    #
    # detalhe_exame = {'peakVoltage': peak_voltage, 'exposureTime': exposure_time, 'tubeCurrent': tube_current,
    #                  'relativeXrayExposure': relative_xray_exposure,
    #                  'ctExposureSequenceGroup': ct_exposure_sequence_group,
    #                  'ctDoseIndex': ct_dose_index}
    #
    # # busca o documento pelo id do exame
    # documento_exame = db.get(id_exame)
    #
    # if not documento_exame:
    #
    #     # Cria o documento com os detalhes do exame
    #     documento_exame = {'_id': id_exame,  # Id do exame como chave do documento
    #                        'tipoDocumento': "DETALHES_EXAME",
    #                        **detalhe_exame}
    #
    #     db.save(documento_exame)


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

    __create_dicom_table_in_hbase()

    root = __validate_dir(sys.argv[1])

    print("Root dir:", root)

    dicom_dict = dict({})

    for path, subdirs, files in os.walk(root):
        for name in files:
            file = os.path.join(path, name)

            file_extension = name.split('.')[-1]
            if file_extension.lower() != 'dcm':
                print('Ignoring file:', file)
                continue

            print('Working in file:', file)

            dicom_dataset = pydicom.dcmread(file)

            compressed_pixel_array = sys.stdout.buffer.write(base64.b64encode(dicom_dataset.pixel_array))

            dicom_dataset_dict = __extract_dataset_to_dict(dicom_dict, dicom_dataset, compressed_pixel_array)

            rowkey, column_family = __define_column_family(dicom_dataset)

            insert_in_hbase(rowkey, column_family, json.dumps(dicom_dataset_dict))

            insert_in_couchdb(rowkey, dicom_dataset)
