import csv

from reader import CSV_FOLDER
from helpers import logger, table_cols, TABLE_NAMES, preprocessing, HEADER_DICT, DBConnector
from sql_scripts import *

class DBPusher(DBConnector):
    def __init__(self, table_names=TABLE_NAMES, table_cols=table_cols, preprocess=preprocessing):
        DBConnector.__init__(self)
        self.first_proc = True
        self.first_enc = True
        self.patient_dict = {}
        self.enc_dict = {}
        self.table_names = table_names
        self.table_cols = table_cols
        self.preprocessing = preprocess
        self.headers_dict = HEADER_DICT

    def create_tables(self):
        logger.info('Tables created here')
        for table in self.table_names:
            table_exists = self.curs.execute(sql_table_exist.format(table_name=table))
            headers = self.headers_dict[table]
            if table_exists.fetchone()[0]:
                self.curs.execute(sql_truncate_table.format(table_name=table))
            else:
                if table == 'patient':
                    self.curs.execute(sql_create_table.format(table_name=table, columns=', '.join(table_cols[table])))
                if table == 'encounter':
                    self.curs.execute(sql_create_table_1fk.format(
                        table_name=table,
                        columns=', '.join(self.table_cols[table]),
                        key_name=headers[1],
                        other_table='patient',
                        other_pk='id'
                    ))
                if table == 'procedure':
                    self.curs.execute(sql_create_table_2fk.format(
                        table_name=table,
                        columns=', '.join(self.table_cols[table]),
                        key_name1=headers[1],
                        other_table1='patient',
                        other_pk1='id',
                        key_name2=headers[2],
                        other_table2='encounter',
                        other_pk2='id'
                    ))
                if table == 'observation':
                    self.curs.execute(sql_create_table_2fk.format(
                        table_name=table,
                        columns=', '.join(self.table_cols[table]),
                        key_name1=headers[1],
                        other_table1='patient',
                        other_pk1='id',
                        key_name2=headers[2],
                        other_table2='encounter',
                        other_pk2='id'
                    ))

            self.db_conection.commit()

    def populate_tables(self):
        for table in TABLE_NAMES:
            table_exists = self.curs.execute(sql_table_exist.format(table_name=table))
            file_name = f"{CSV_FOLDER}/{table.capitalize()}.csv"
            headers = self.headers_dict[table]
            logger.info(f'Filling out table {table}')
            if table_exists.fetchone()[0] and table_cols.get(table):
                with open(file_name, newline='') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        if row[0] == 'source_id':
                            continue

                        if table == 'encounter':
                            if self.first_enc:
                                self.first_enc = False

                                patient_raw = self.curs.execute(sql_select_all.format(
                                    column_names='id, source_id',
                                    table_name='patient'))
                                self.patient_dict = {v: k for (k, v) in patient_raw.fetchall()}

                            pat_id = row[1].split('/')[1]
                            row[1] = str(self.patient_dict[pat_id])

                        if table in ('procedure', 'observation'):
                            if self.first_proc:
                                self.first_proc = False

                                enc_raw = self.curs.execute(sql_select_all.format(
                                    column_names='id, source_id',
                                    table_name='encounter'))
                                self.enc_dict = {v: k for (k, v) in enc_raw.fetchall()}

                            pat_id = row[1].split('/')[1]
                            row[1] = str(self.patient_dict[pat_id])
                            if row[2]:
                                enc_id = row[2].split('/')[1]
                                row[2] = str(self.enc_dict.get(enc_id, ''))

                        self.curs.execute(sql_insert_values.format(col_names=', '.join(headers),
                                                                   table_name=table,
                                                                   csv_row=self.preprocessing(','.join(row))))

            self.db_conection.commit()

if __name__ == '__main__':
    logger.info('Creating and populating tables started here')
    db_pusher = DBPusher()
    db_pusher.connect()
    db_pusher.create_tables()
    db_pusher.populate_tables()
    db_pusher.disconnect()
    logger.info('Creating and populating tables ended here')
