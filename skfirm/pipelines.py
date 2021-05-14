# -*- coding: utf-8 -*-

import os
from io import BytesIO

import urllib.request, urllib.parse, urllib.error

from scrapy.utils.misc import md5sum
from scrapy.pipelines.files import FilesPipeline

class FirmwarePipeline(FilesPipeline):

    # overrides initial function from FilesPipeline
    def __init__(self, store_uri, download_func=None, settings=None):
        if settings and "SQL_SERVER" in settings:
            import psycopg2
            self.database = psycopg2.connect(database="firmware", user="firmadyne",
                                             password="firmadyne", host=settings["SQL_SERVER"],
                                             port=5432)
        else:
            self.database = None

        super(FirmwarePipeline, self).__init__(store_uri, download_func,settings)

    def test(self):
        print("hi!")

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
        if not os.path.isfile(path):
            buf.seek(0)
            self.store.persist_file(path, buf, info)
        return checksum
