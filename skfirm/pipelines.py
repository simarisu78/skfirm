# -*- coding: utf-8 -*-

from scrapy.exceptions import DropItem
from scrapy.http import Request
import os
from io import BytesIO
import urllib.request, urllib.parse, urllib.error
from scrapy.utils.misc import md5sum
from scrapy.pipelines.files import FilesPipeline
import logging
from pymongo import MongoClient
import time

logger = logging.getLogger(__name__)

class FirmwarePipeline(FilesPipeline):

    # Called when the spider starts
    # connect to MongoDB
    def open_spider(self, spider):
        self.spiderinfo = self.SpiderInfo(spider)
        logger.debug("open_spider")
        self.client = MongoClient('localhost', 27017)
        self.db = self.client['firmware']
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
    def file_path(self, request, response=None, info=None, *, item):
        # vendor/category/productname/version_date[_number].zip .bin (etc)
        # If the file already exists and has a different hash, add a number to the end of the file name.

        if item.get('version') is None:
            item['version'] = "v1"
        if item.get('date') is None:
            item['date'] = "0000"

        parsed_url = urllib.parse.urlparse(urllib.parse.unquote(request.url)).path
        filename = parsed_url[parsed_url.rfind("/") + 1:]
        if request.meta.get("isGpl"):
            return "%s/%s/%s/gpl/%s/%s" % (item.get('vendor'), item.get('category'),item.get('product'), item.get('version'), filename)
        return "%s/%s/%s/%s_%s/%s" % (item.get('vendor'), item.get('category'),item.get('product'), item.get('version'), item.get('date'), filename)

    # overrides function from FilesPipeline
    def file_downloaded(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        buf = BytesIO(response.body)
        checksum = md5sum(buf)

        # そのうち英訳します
        # データベースに同じハッシュがあるかチェックし、あればファイル保存を見送るようにする
        if self.client:
            try:
                ret = self.collection.count_documents(filter={'vendor':item.get('vendor'), 'checksum':checksum})
            except BaseException as e:
                logger.critical("Database connection exception!: %s" %e)
                raise

        first = path[0:path.rfind("/")+1]
        end = path[path.rfind("/")+1:]
        checksum_path = first+checksum+"_"+end
        if not os.path.isfile(self.store._get_filesystem_path(checksum_path)) and ret==0:
            logger.debug("checksum not found! persist file %s" % checksum_path)
            buf.seek(0)
            self.store.persist_file(checksum_path, buf, info)
        return checksum

   # overrides function from FilesPipeline
    def get_media_requests(self, item, info):
        # check for mandatory fields
        for x in ["vendor"]:
            if x not in item:
                raise DropItem(
                    "Missing required field '%s' for item: " % x)


        # resolve dynamic redirects in urls
        for x in ["mib", "gpl", "url"]:
            if x in item:
                split = urllib.parse.urlsplit(item[x])
                # remove username/password if only one provided
                if split.username or split.password and not (split.username and split.password):
                    item[x] = urllib.parse.urlunsplit(
                        (split[0], split[1][split[1].find("@") + 1:], split[2], split[3], split[4]))

                if split.scheme == "http":
                    item[x] = urllib.request.urlopen(item[x]).geturl()

        # check for filtered url types in path
        if "url" in item:
            url = urllib.parse.urlparse(item["url"])
        elif "gpl" in item:
            url = urllib.parse.urlparse(item["gpl"])

        if any(url.path.endswith(x) for x in [".pdf", ".php", ".txt", ".doc", ".rtf", ".docx", ".htm", ".html", ".md5", ".sha1", ".torrent"]):
            raise DropItem("Filtered path extension: %s" % url.path)
        elif any(x in url.path for x in ["driver", "utility", "install", "wizard", "login"]):
            raise DropItem("Filtered path type: %s" % url.path)

        # generate list of url's to download
        item['file_urls'] = [item[x] for x in ["mib", "url", "gpl"] if x in item]

        #logger.debug(item['file_urls'])
        # pass vendor so we can generate the correct file path and name
        #return [Request(x, meta={"vendor": item["vendor"]}) for x in item['file_urls']]
        #メタ情報の充実のため、実際には一つのアイテムには一つのurlもしくはgplしか入っていない
        #念の為残すが、可読性のために消すかも
        for file_url in item['file_urls']:
            if "gpl" in item:
                yield Request(file_url, meta={"vendor": item["vendor"], "isGpl":True, "isFile":True})
                continue
            yield Request(file_url, meta={"vendor": item["vendor"], "isFile":True})

    # overrides function from FilesPipeline
    def item_completed(self, results, item, info):
        item['files'] = []
        if isinstance(item, dict) or 'files' in item.fields:
            item['files'] = [x for ok, x in results if ok]

        item['checksum'] = item['files'][0]['checksum']
        del item['files']
        del item['file_urls']
        
        if self.client:
            try:
                search_count = self.collection.count_documents({"checksum":item['checksum']})
                if(search_count != 0):
                    logger.debug("firmware was already downloaded. : %s" % item['checksum'])
                    return

                logger.debug("firmware is not downloaded")
                copy = item.deepcopy()
                self.collection.insert_one(dict(copy))
                return item
            except BaseException as e:
                logger.critical("Database connection exception!: %s" %e)
                raise
