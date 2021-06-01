import scrapy
import re
from skfirm.items import FirmwareItem
from skfirm.loader import FirmwareLoader
from itemloaders.processors import Identity, MapCompose, TakeFirst
import urllib.parse

class TpLinkSpider(scrapy.Spider):
    name = 'tp-link'
    vendor = 'tp-link'
    #allowed_domains = ['www.tp-link.com']
    start_urls = ['https://www.tp-link.com/jp/support/download/']
    item_lists = []
    count = 0

    ustom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }
    
    def parse(self, response):
        product_types = response.xpath("//div[@class='item']")
        for i, product_group in enumerate(product_types):
            category = product_group.xpath(".//span[@class='tp-m-show']/text()").get().strip()
            
            product_lists = product_group.xpath(".//a")
            for j, product in enumerate(product_lists):
                model = product.xpath(".//text()").get().strip()
                next_link =  product.xpath(".//@href").get().strip()
                if i == len(product_types)-1 and j == len(product_lists)-1:
                    yield response.follow(next_link, meta={"category": category, "product":model, "isLast":True}, callback=self.parse_version)
                else:
                    yield response.follow(next_link, meta={"category": category, "product":model}, callback=self.parse_version)

    def parse_version(self, response):
        hardWareVer = response.xpath("//dl[@class='select-version']/.//a/@href").getall()
        category = response.meta.get("category")
        model = response.meta.get("product")
        for i, hwVer in enumerate(hardWareVer):
            if i == len(hardWareVer)-1:
                #print(response.meta.get("isLast"))
                if response.meta.get("isLast"):
                    print("Taken!                         : %s \n\n\n\n\n" % hwVer)
                    yield scrapy.Request(hwVer, meta={"isLast":True, "category": category, "product":model}, callback=self.parse_product)
                    return
                else:
                    #print("Not Taken!                         : %s \n\n\n" % hwVer)
                    yield response.follow(hwVer, meta={"category": category, "product":model}, callback=self.parse_product)
                    return
            else:
                yield response.follow(hwVer, meta={"isLast":False, "category": category, "product":model}, callback=self.parse_product)
        if hardWareVer is None:
            yield response.request
                
    def parse_product(self, response):
        self.logger.info("Parsing %s..." % response.url)
        self.logger.info("meta: %s" % response.meta)
        TpLinkSpider.count += 1
        tables = response.xpath("//div[@id='content_Firmware']/table")
        self.logger.info("%s %s : %d binary firmware found." % (response.meta["category"], response.meta["product"], len(tables)))
        
        for firmware in tables:
            reg_version = re.search(r'_(V.+)_',firmware.xpath(".//a/text()").get())
            spans = firmware.xpath(".//tr[@class='detail-info']/td/span/text()")
            item = FirmwareLoader(item=FirmwareItem(), response=response, date_fmt=["%Y-%m-%d"],description_in = Identity())
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
            TpLinkSpider.item_lists.append(item.load_item())

        gpl_source_codes = response.xpath("//div[@id='content_GPL-Code']/.//a")
        response.meta["isGpl"] = True
        self.logger.info("     %s : %d gpl source code found." % (response.meta["product"], len(gpl_source_codes)))
        for gpl in gpl_source_codes:
            item = FirmwareLoader(
            item=FirmwareItem(), response=response, date_fmt=["%Y-%m-%d"])
            item.add_value("vendor", self.vendor)
            item.add_value("gpl", gpl.xpath("./@href").get())
            item.add_value("product", response.meta["product"])
            item.add_value("category", response.meta["category"])
            gpl_ver = re.search("(V.?)", gpl.xpath("./text()").get())
            if gpl_ver is not None:
                item.add_value("version", gpl_ver.groups()[0])
            TpLinkSpider.item_lists.append(item.load_item())

        if response.meta.get("isLast"):
            import time
            self.logger.info("files! : %d" % len(TpLinkSpider.item_lists))
            for itm in TpLinkSpider.item_lists:
                yield itm
                #time.sleep(90)
                #AutoThrottleの変更で対応する
                #sleepだと全体の処理が止まってしまうのでダメです
