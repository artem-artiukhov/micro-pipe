#!/usr/bin/env python3
import os
import shutil

from downloader import Downloader
from reader import Reader
from db_pusher import DBPusher
from db_getter import Reporter
from helpers import logger, DEST_FOLDER, CSV_FOLDER, db_name

logger.info('*********************')
logger.info('* PIPE STARTED HERE *')
logger.info('*********************')
logger.info('Downloading of ndjson files started')
d = Downloader()
d.file_names_getter()
d.json_downloader()

logger.info('Reading and translating nbjson to CSV started')
r = Reader()
r.valid_files()
r.write_files()

logger.info('Creating and populating tables started here')
db_pusher = DBPusher()
db_pusher.connect()
db_pusher.create_tables()
db_pusher.populate_tables()
db_pusher.disconnect()

logger.info('********************')
logger.info('* REPORT ON TABLES *')
logger.info('********************')
db_getter = Reporter()
db_getter.connect()
db_getter.patients()
db_getter.procedures()
db_getter.encounters()
db_getter.disconnect()
logger.info('*******************')
logger.info('* PIPE ENDED HERE *')
logger.info('*******************')

# removing all files pipe worked with
logger.info('Removing temporary folder with ndjson files...')
shutil.rmtree(DEST_FOLDER)
logger.info('Removing temporary folder with csv files...')
shutil.rmtree(CSV_FOLDER)
logger.info('Removing local database storage...')
os.remove(db_name)
logger.info('Everything is cleaned up. Enjoy your day!')