#!/usr/bin/env python3

from downloader import Downloader
from reader import Reader
from db_pusher import DBPusher
from db_getter import Reporter
from helpers import logger

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