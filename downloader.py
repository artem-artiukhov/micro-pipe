import urllib.request
import re
import os
import asyncio

import aiohttp

from helpers import logger, URL, RAW_FILES_DOMAIN, RAW_FILES_URL, HTTP_DOMAIN, FILE_PAGE, FILE_NAME, DEST_FOLDER

class Downloader:
    def __init__(self, domain=RAW_FILES_DOMAIN, page=FILE_PAGE, dest_folder=DEST_FOLDER, temp_file=FILE_NAME):
        self.file_names = []
        self.domain = domain
        self.page = page
        self.dest_folder = dest_folder
        self.url = self.domain + self.page
        self.temp_file = temp_file

    def _check(self, path):
        return os.path.exists(path)

    def file_names_getter(self):
        logger.info('Looking for names')
        if not self._check(self.temp_file):
            with urllib.request.urlopen(URL) as resp:
                with open(self.temp_file, 'w') as file:
                    file.write(resp.read().decode('utf-8'))

        json_lines = (line for line in open(self.temp_file, 'r').readlines() if 'ndjson' in line)
        for line in json_lines:
            file_name = re.search('title="(\w+.ndjson)"', line).group(1)
            self.file_names.append(file_name)
        logger.info('Names received')

    async def get_json(self, name):
        async with aiohttp.ClientSession() as session:
            resp = await session.get(f'{self.url}/{name}')
            json_file = await resp.read()
            return json_file

    async def download_one(self, name):
        json_file = await self.get_json(name)
        path = os.path.join(self.dest_folder, name)
        try:
            with open(path, 'wb') as fjson:
                fjson.write(json_file)
        except FileNotFoundError:
            os.mkdir(self.dest_folder)
            with open(path, 'wb') as fjson:
                fjson.write(json_file)

    def json_downloader(self):
        logger.info('Downloading started')
        loop = asyncio.get_event_loop()
        to_do = [self.download_one(name) for name in self.file_names]
        wait_coro = asyncio.wait(to_do)
        res, _ = loop.run_until_complete(wait_coro)
        loop.close()
        logger.info('Downloading finished')

if __name__ == '__main__':
    logger.info('Downloading started here')
    d = Downloader()
    d.file_names_getter()
    d.json_downloader()
    logger.info('Downloading ended here')

