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

logger = logging.getLogger(__name__)

class FirmwarePipeline(FilesPipeline):

    # Called when the spider starts
    # connect to MongoDB
    def open_spider(self, spider):
        self.spiderinfo = self.SpiderInfo(spider)
        logger.debug("open_spider")
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
    def media_to_download(self, request, info, *, item=None):
        def _onsuccess(result):
            if not result:
                return  # returning None force download

            last_modified = result.get('last_modified', None)
            if not last_modified:
                return  # returning None force download

            age_seconds = time.time() - last_modified
            age_days = age_seconds / 60 / 60 / 24
            if age_days > self.expires:
                return  # returning None force download

            referer = referer_str(request)
            logger.debug(
                'File (uptodate): Downloaded %(medianame)s from %(request)s '
                'referred in <%(referer)s>',
                {'medianame': self.MEDIA_NAME, 'request': request,
                 'referer': referer},
                extra={'spider': info.spider}
            )
            self.inc_stats(info.spider, 'uptodate')

            checksum = result.get('checksum', None)
            return {'url': request.url, 'path': path, 'checksum': checksum, 'status': 'uptodate'}

        path = self.file_path(request, response=response, info=info, item=item)
        dfd = defer.maybeDeferred(self.store.stat_file, path, info)
        dfd.addCallbacks(_onsuccess, lambda _: None)
        dfd.addErrback(
            lambda f:
            logger.error(self.__class__.__name__ + '.store.stat_file',
                         exc_info=failure_to_exc_info(f),
                         extra={'spider': info.spider})
        )
        return dfd

    # overrides function from FilesPipeline
    def file_downloaded(self, response, request, info, *, item=None):
        path = self.file_path(request, response=response, info=info, item=item)
        buf = BytesIO(response.body)
        checksum = md5sum(buf)
        if not os.path.isfile(self.store._get_filesystem_path(path)):
            buf.seek(0)
            self.store.persist_file(path, buf, info)
        logger.debug("file_path : %s" % path)
        return checksum

   # overrides function from FilesPipeline
    def get_media_requests(self, item, info):
        logger.debug("get_media_requests")
        # check for mandatory fields
        for x in ["vendor", "url"]:
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
        url = urllib.parse.urlparse(item["url"])
        if any(url.path.endswith(x) for x in [".pdf", ".php", ".txt", ".doc", ".rtf", ".docx", ".htm", ".html", ".md5", ".sha1", ".torrent"]):
            raise DropItem("Filtered path extension: %s" % url.path)
        elif any(x in url.path for x in ["driver", "utility", "install", "wizard", "login"]):
            raise DropItem("Filtered path type: %s" % url.path)

        # generate list of url's to download
        item['file_urls'] = [item[x] for x in ["mib", "url", "gpl"] if x in item]

        logger.debug(item['file_urls'])
        # pass vendor so we can generate the correct file path and name
        #return [Request(x, meta={"vendor": item["vendor"]}) for x in item['file_urls']]
        for file_url in item['file_urls']:
            logger.debug("file_url: %s " % file_url)
            yield Request(file_url, meta={"vendor": item["vendor"]})

    # overrides function from FilesPipeline
    def item_completed(self, results, item, info):
        logger.debug("item_completed")
        logger.debug(results)
        item['files'] = []
        if isinstance(item, dict) or 'files' in item.fields:
            item['files'] = [x for ok, x in results if ok]

        """
        mongoDB 4.0以降じゃないとトランザクション処理ができません！！！！
        なのでとりあえずエラー処理は後回し
        """
        if self.client:
            try:
                copy = item.deepcopy()
                self.collection.insert_one(dict(copy))
                return item
            except BaseException as e:
                logger.critical("Database connection exception!: %s" %e)
                raise
