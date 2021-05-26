import scrapy


class TpLinkSpider(scrapy.Spider):
    name = 'tp-link'
    allowed_domains = ['www.tp-link.com']
    start_urls = ['http://www.tp-link.com/']

    def parse(self, response):
        pass
