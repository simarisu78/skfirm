# -*- coding: utf-8 -*-

import os
from io import BytesIO
import urllib.request, urllib.parse, urllib.error
from scrapy.utils.misc import md5sum
from scrapy.pipelines.files import FilesPipeline
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

class FirmwarePipeline(FilesPipeline):

    # Called when the spider starts
    # connect to MongoDB
    def open_spider(self, spider):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['test-scrapy']
        self.collection = self.db['items']

    # Called when the spider close
    # disconnect to MongoDB
    def close_spider(self, spider):
        self.client.close()

    # calculate file's checksum(md5)
    # scrapy.pipelines.files - FilesPipeline - file_downloaded
    # https://github.com/scrapy/scrapy/blob/06f3d12c1208c380f9f1a16cb36ba2dfa3c244c5/scrapy/pipelines/files.py
    def get_md5sum(self, response):
        buf = BytesIO(response.body)
        checksum = md5sum(buf)
        return checksum

    # overrides fuction from FilesPipeline
    def file_path(self, request, response, info=None, *, item):
        extension = os.path.splitext(os.path.basename(
            urllib.parse.urlsplit(request.url).path))[1]
        # vendor/category/productname/version_date_checksum.zip .bin (etc)
        # if spiders can't find version, file name is only a checksum.
        if item.get('version') is None:
            item['version'] = "v1"
        if item.get('date') is None:
            item['date'] = "0000"
        return "%s/%s/%s/%s_%s_%s%s" % (item.get('vendor'), item.get('category'),
                item.get('product'), item.get('version'), item.get('date'), self.get_md5sum(response), extension)

    # overrides function from FilesPipeline
    def file_downloaded(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        buf = BytesIO(response.body)
        checksum = md5sum(buf)
        if not os.path.isfile(self.store._get_filesystem_path(path)):
            buf.seek(0)
            self.store.persist_file(path, buf, info)
        return checksum

    # overrides function from FilesPipeline
    def item_completed(self, results, item, info):
        item['files'] = []
        if isinstance(item, dict) or files in item.fields:
            item['files'] = [x for ok, x in results if ok]

"""
mongoDB 4.0以降じゃないとトランザクション処理ができません！！！！
なのでとりあえずエラー処理は後回し
"""
        #s = client.start_session()
        if self.client:
            try:
                copy = item.deepcopy()
                self.collection.insert_one(dict(copy))
                return items
            except BaseException as e:
                logger.critical("Database connection exception!: $s" %e)
                raise
