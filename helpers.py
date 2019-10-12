import logging
import sqlite3

conn = sqlite3.connect('test.db')
conn.isolation_level = 'EXCLUSIVE'

logger = logging.getLogger("Simple ETL logger")
logging.basicConfig(format='[%(levelname)s] at %(asctime)s %(message)s',
                    datefmt='%d-%m-%Y %H:%M:%S',
                    # filename='downloader.log',
                    # filemode='w',
                    level=logging.INFO)

pat_headers = ['source_id', 'birth_date', 'gender', 'race_code', 'race_code_system', 'ethnicity_code',
               'ethnicity_code_system', 'country']
enc_headers = ['source_id', 'patient_id', 'start_date', 'end_date', 'type_code', 'type_code_system']
proc_headers = ['source_id', 'patient_id', 'encounter_id', 'procedure_date', 'type_code', 'type_code_system']
obs_headers = ['source_id', 'patient_id', 'encounter_id', 'observation_date', 'type_code', 'type_code_system', 'value',
               'unit_code', 'unit_code_system']

CSV_FOLDER = 'csv/'

db_name = 'test.db'

URL = 'https://github.com/smart-on-fhir/flat-fhir-files/tree/master/r3'
RAW_FILES_URL = 'https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3'

HTTP_DOMAIN = 'https://github.com'
RAW_FILES_DOMAIN = 'https://raw.githubusercontent.com'
FILE_PAGE = '/smart-on-fhir/flat-fhir-files/master/r3'

FILE_NAME = '/tmp/page.html'
DEST_FOLDER = 'jsons'

TABLE_NAMES = ['patient', 'encounter', 'procedure', 'observation']

HEADER_DICT = {'patient': pat_headers, 'encounter': enc_headers, 'procedure': proc_headers, 'observation': obs_headers}

table_cols = {
    'patient': [
        ' '.join([pat_headers[0], 'TEXT']),
        ' '.join([pat_headers[1], 'DATE']),
        ' '.join([pat_headers[2], 'TEXT']),
        ' '.join([pat_headers[3], 'TEXT']),
        ' '.join([pat_headers[4], 'TEXT']),
        ' '.join([pat_headers[5], 'TEXT']),
        ' '.join([pat_headers[6], 'TEXT']),
        ' '.join([pat_headers[7], 'TEXT']),
    ],
    'encounter': [
        ' '.join([enc_headers[0], 'TEXT']),
        ' '.join([enc_headers[1], 'INTEGER']),
        ' '.join([enc_headers[2], 'DATETIME']),
        ' '.join([enc_headers[3], 'DATETIME']),
        ' '.join([enc_headers[4], 'TEXT']),
        ' '.join([enc_headers[5], 'TEXT']),
    ],
    'procedure': [
        ' '.join([proc_headers[0], 'TEXT']),
        ' '.join([proc_headers[1], 'INTEGER']),
        ' '.join([proc_headers[2], 'INTEGER']),
        ' '.join([proc_headers[3], 'DATE']),
        ' '.join([proc_headers[4], 'TEXT']),
        ' '.join([proc_headers[5], 'TEXT']),
    ],
    'observation': [
        ' '.join([obs_headers[0], 'TEXT']),
        ' '.join([obs_headers[1], 'INTEGER']),
        ' '.join([obs_headers[2], 'INTEGER']),
        ' '.join([obs_headers[3], 'DATE']),
        ' '.join([obs_headers[4], 'TEXT']),
        ' '.join([obs_headers[5], 'TEXT']),
        ' '.join([obs_headers[6], 'DECIMAL']),
        ' '.join([obs_headers[7], 'TEXT']),
        ' '.join([obs_headers[8], 'TEXT']),
    ],
}


def preprocessing(line):
    processed_line = []
    for word in line.split(','):
        if type(word) == int:
            processed_line.append(str(word))
        else:
            processed_line.append(f'"{word}"')
    return ','.join(processed_line)

class DBConnector:
    def __init__(self):
        self.db_conection = None
        self.curs = None

    def connect(self, db=db_name):
        self.db_conection = sqlite3.connect(db)
        self.db_conection.isolation_level = 'EXCLUSIVE'
        self.curs = self.db_conection.cursor()

    def disconnect(self):
        self.db_conection.close()