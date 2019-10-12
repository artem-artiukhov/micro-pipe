import os
import json
import csv

from downloader import DEST_FOLDER
from helpers import logger, pat_headers, enc_headers, obs_headers, proc_headers, CSV_FOLDER

class Reader:
    def __init__(self, source_folder=DEST_FOLDER, dest_folder=CSV_FOLDER):
        self.file_names = []
        self.dest_folder = dest_folder
        self.src_folder = source_folder

    def _check(self, path):
        return os.path.exists(path)

    def valid_files(self):
        logger.info('Selecting of valid files started here')
        try:
            with os.scandir(self.src_folder) as entries:
                for file in entries:
                    info = file.stat()
                    if info.st_size != 0:
                        self.file_names.append(file.name)
            logger.info('Selecting of valid files finished here')
        except FileNotFoundError as e:
            logger.error(f'{e.strerror} ({self.src_folder})')
            os._exit(250)

    def write_files(self):
        logger.info('Saving of required files started here')
        if not self._check(self.dest_folder):
            os.mkdir(self.dest_folder)

        for file in self.file_names:
            if file.startswith('Encounter'):
                with open(self.dest_folder + file.split('.')[0] + '.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=enc_headers)
                    writer.writeheader()
                    with open(self.src_folder + '/' + file) as f:
                        for line in f:
                            loaded_data = json.loads(line, encoding='UTF-8')
                            writer.writerow({
                                enc_headers[0]: loaded_data['id'],
                                enc_headers[1]: loaded_data['subject']['reference'],
                                enc_headers[2]: loaded_data['period']['start'],
                                enc_headers[3]: loaded_data['period']['end'],
                                enc_headers[4]: loaded_data['type'][0]['coding'][0]["code"],
                                enc_headers[5]: loaded_data['type'][0]['coding'][0]["system"]
                            })

            if file.startswith('Patient'):
                with open(self.dest_folder + file.split('.')[0] + '.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=pat_headers)
                    writer.writeheader()
                    with open(self.src_folder + '/' + file) as f:
                        for line in f:
                            loaded_data = json.loads(line, encoding='UTF-8')

                            race_code = ''
                            race_code_system = ''
                            etnicity_code = ''
                            etnicity_code_system = ''
                            if loaded_data.get('extension'):
                                for ext in loaded_data.get('extension'):
                                    if ext['url'] == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race':
                                        race_code = ext['valueCodeableConcept']['coding'][0]['code']
                                        race_code_system = ext['valueCodeableConcept']['coding'][0]['system']
                                    if ext[
                                        'url'] == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity':
                                        etnicity_code = ext['valueCodeableConcept']['coding'][0]['code']
                                        etnicity_code_system = ext['valueCodeableConcept']['coding'][0]['system']

                            writer.writerow({
                                pat_headers[0]: loaded_data['id'],
                                pat_headers[1]: loaded_data['birthDate'],
                                pat_headers[2]: loaded_data['gender'],
                                pat_headers[3]: race_code,
                                pat_headers[4]: race_code_system,
                                pat_headers[5]: etnicity_code,
                                pat_headers[6]: etnicity_code_system,
                                pat_headers[7]: loaded_data['address'][0]['country']
                                if (loaded_data.get('address')
                                    and loaded_data['address'][0].get('country')) else '',
                            })

            if file.startswith('Procedure'):
                with open(self.dest_folder + file.split('.')[0] + '.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=proc_headers)
                    writer.writeheader()
                    with open(self.src_folder + '/' + file) as f:
                        for line in f:
                            loaded_data = json.loads(line, encoding='UTF-8')
                            writer.writerow({
                                proc_headers[0]: loaded_data['id'],
                                proc_headers[1]: loaded_data['subject']['reference'] if loaded_data.get(
                                    'subject') else '',
                                proc_headers[2]: loaded_data['context']['reference'] if loaded_data.get(
                                    'context') else '',
                                proc_headers[3]: loaded_data['performedDateTime'] if loaded_data.get(
                                    'performedDateTime')
                                else loaded_data['performedPeriod']['start'],
                                proc_headers[4]: loaded_data['code']['coding'][0]['code'],
                                proc_headers[5]: loaded_data['code']['coding'][0]['system'],
                            })

            if file.startswith('Observation'):
                with open(self.dest_folder + file.split('.')[0] + '.csv', 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=obs_headers)
                    writer.writeheader()
                    with open(self.src_folder + '/' + file) as f:
                        for line in f:
                            loaded_data = json.loads(line, encoding='UTF-8')
                            if loaded_data.get('component'):
                                for comp in loaded_data.get('component'):
                                    writer.writerow({
                                        obs_headers[0]: loaded_data['id'],
                                        obs_headers[1]: loaded_data['subject']['reference'] if loaded_data.get(
                                            'subject') else '',
                                        obs_headers[2]: loaded_data['context']['reference'] if loaded_data.get(
                                            'context') else '',
                                        obs_headers[3]: loaded_data['effectiveDateTime'] if loaded_data.get(
                                            'effectiveDateTime') else '',
                                        obs_headers[4]: comp['code']['coding'][0]['code'],
                                        obs_headers[5]: comp['code']['coding'][0]['system'],
                                        obs_headers[6]: comp['valueQuantity']['value'] if comp.get(
                                            'valueQuantity') else '',
                                        obs_headers[7]: comp['valueQuantity']['unit'] if comp.get(
                                            'valueQuantity') else '',
                                        obs_headers[8]: comp['valueQuantity']['system'] if comp.get(
                                            'valueQuantity') else '',
                                    })
                            else:
                                writer.writerow({
                                    obs_headers[0]: loaded_data['id'],
                                    obs_headers[1]: loaded_data['subject']['reference'] if loaded_data.get(
                                        'subject') else '',
                                    obs_headers[2]: loaded_data['context']['reference'] if loaded_data.get(
                                        'context') else '',
                                    obs_headers[3]: loaded_data['effectiveDateTime'] if loaded_data.get(
                                        'effectiveDateTime') else '',
                                    obs_headers[4]: loaded_data['code']['coding'][0]['code'],
                                    obs_headers[5]: loaded_data['code']['coding'][0]['system'],
                                    obs_headers[6]: loaded_data['valueQuantity']['value'] if loaded_data.get(
                                        'valueQuantity') else '',
                                    obs_headers[7]: loaded_data['valueQuantity']['unit'] if loaded_data.get(
                                        'valueQuantity')
                                                                                            and loaded_data[
                                                                                                'valueQuantity'].get(
                                        'unit') else '',
                                    obs_headers[8]: loaded_data['valueQuantity']['system'] if loaded_data.get(
                                        'valueQuantity')
                                                                                              and loaded_data[
                                                                                                  'valueQuantity'].get(
                                        'system') else '',
                                })

        logger.info('Saving of required files ended here')

if __name__ == '__main__':
    logger.info('Reading and translating to CSV started here')
    r = Reader()
    r.valid_files()
    r.write_files()
