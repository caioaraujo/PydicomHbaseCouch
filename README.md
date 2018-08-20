# DICOM HBase - CouchDB
Importa arquivos padrão DICOM (*.dcm) para bases de dados hbase e couchdb.

## Pré-requisitos
- Python 3.5 ou posterior;
- Gerenciador de pacotes do Python (pip);
- hbase-1.2.1 ou posterior;
- couchdb 1.6.0 ou posterior.

Obs: Para instalar o gerenciador de pacotes no Ubuntu:

```sudo apt install python3-pip```

## Instalando as dependências
```pip3 install -r requirements.txt --user```

## Execução
Para executar o script:

```python3 script.py "caminho/pasta/raiz/arquivos/dicom"```
