import logging

from scrapy.exceptions import NotConfigured
from scrapy import signals

logger = logging.getLogger(__name__)


class FileThrottle(object):

    def __init__(self, crawler):
        self.crawler = crawler
        if not crawler.settings.getfloat('FILETHROTTLE_ENABLED'):
            raise NotConfigured

        self.fileDelay = crawler.settings.getfloat('FILE_DELAY')
        self.defaultDelay = crawler.settings.getfloat('DOWNLOAD_DELAY')
        crawler.signals.connect(self._request_reached_downloader , signal=signals.request_reached_downloader)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def _get_slot(self, request, spider):
        key = request.meta.get('download_slot')
        return key, self.crawler.engine.downloader.slots.get(key)
    
    def _request_reached_downloader(self, request, spider):
        key, slot = self._get_slot(request, spider)
        if slot is None:
            logger.debug("slot %s not found" % key)
            raise

        olddelay = slot.delay
        if request.meta.get('isFile'):
            slot.delay = self.fileDelay
        else:
            slot.delay = self.defaultDelay
            
        if olddelay != slot.delay:
            logger.debug("delay has changed (%f to %f)" % (olddelay, slot.delay))
