import scrapy
import re
from skfirm.items import FirmwareItem
from skfirm.loader import FirmwareLoader
from itemloaders.processors import Identity, MapCompose, TakeFirst

class TpLinkSpider(scrapy.Spider):
    name = 'tp-link'
    vendor = 'tp-link'
    allowed_domains = ['www.tp-link.com']
    start_urls = ['https://www.tp-link.com/jp/support/download/']

    def parse(self, response):
        for product_group in response.xpath("//div[@class='item']"):
            category = product_group.xpath(".//span[@class='tp-m-show']/text()").get().strip()

            for product in product_group.xpath(".//a"):
                model = product.xpath(".//text()").get().strip()
                next_link =  product.xpath(".//@href").get().strip()
                yield response.follow(next_link, meta={"category": category, "product":model}, callback=self.parse_product)

    def parse_product(self, response):
        self.logger.debug("Parsing %s..." % response.url)

        tables = response.xpath("//div[@id='content_Firmware']/table")
        self.logger.debug("%s %s : %d binary firmware found." % (response.meta["category"], response.meta["product"], len(tables)))

        for firmware in tables:
            reg_version = re.search(r'_(V.+)_',firmware.xpath(".//a/text()").get())
            spans = firmware.xpath(".//tr[@class='detail-info']/td/span/text()")
            item = FirmwareLoader(item=FirmwareItem(), response=response, date_fmt=["%y-%m-%d"],description_in = Identity())
            item.add_value("vendor", self.vendor)
            item.add_value("url", firmware.xpath(".//a/@href").get())
            item.add_value("date", spans[1].get().strip())
            item.add_value("language", spans[3].get().strip())
            item.add_value("size", spans[5].get().strip())
            item.add_value("description", "\n".join(firmware.xpath(".//td[@class='more']/.//p").getall()))
            item.add_value("product", response.meta["product"])
            item.add_value("category", response.meta["category"])
            if reg_version is not None:
                item.add_value("version", reg_version.groups()[0])
            yield item.load_item()
