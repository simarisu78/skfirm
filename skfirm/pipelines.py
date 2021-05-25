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
    def get_media_requests(self, item, info):
        # check for mandatory fields
        for x in ["vendor", "url"]:
            if x not in item:
                raise DropItem(
                    "Missing required field '%s' for item: " % x)

        # resolve dynamic redirects in urls
        for x in ["mib", "sdk", "url"]:
            if x in item:
                split = urllib.parse.urlsplit(item[x])
                # remove username/password if only one provided
                if split.username or split.password and not (split.username and split.password):
                    item[x] = urllib.parse.urlunsplit(
                        (split[0], split[1][split[1].find("@") + 1:], split[2], split[3], split[4]))

                if split.scheme == "http":
                    item[x] = urllib.request.urlopen(item[x]).geturl()

        # check for filtered url types in path
        url = urllib.parse.urlparse(item["url"])
        if any(url.path.endswith(x) for x in [".pdf", ".php", ".txt", ".doc", ".rtf", ".docx", ".htm", ".html", ".md5", ".sha1", ".torrent"]):
            raise DropItem("Filtered path extension: %s" % url.path)
        elif any(x in url.path for x in ["driver", "utility", "install", "wizard", "gpl", "login"]):
            raise DropItem("Filtered path type: %s" % url.path)

        # generate list of url's to download
        item[self.files_urls_field] = [item[x]
                                       for x in ["mib", "url"] if x in item]

        # pass vendor so we can generate the correct file path and name
        return [Request(x, meta={"ftp_user": "anonymous", "ftp_password": "chrome@example.com", "vendor": item["vendor"]}) for x in item[self.files_urls_field]]

    # overrides function from FilesPipeline
    def item_completed(self, results, item, info):
        item['files'] = []
        if isinstance(item, dict) or files in item.fields:
            item['files'] = [x for ok, x in results if ok]

        """
        mongoDB 4.0以降じゃないとトランザクション処理ができません！！！！
        なのでとりあえずエラー処理は後回し
        """
        if self.client:
            try:
                copy = item.deepcopy()
                self.collection.insert_one(dict(copy))
                return items
            except BaseException as e:
                logger.critical("Database connection exception!: $s" %e)
                raise
