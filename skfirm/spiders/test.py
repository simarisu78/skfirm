from scrapy import Spider
from scrapy.http import Request

from skfirm.items import FirmwareItem
from skfirm.loader import FirmwareLoader

class BuffaloSpider(Spider):
    name = "buffalo"
    allowed_domains = ["buffalotech.com", "cdn.cloudfiles.mosso.com"]
    start_urls = ["https://www.buffalotech.com/products/airstation-highpower-n300-open-source-dd-wrt-wireless-router"]

    def parse(self, response):
            yield from response.follow_all(
                xpath='//article/div/a',
                callback=self.parse_product)


    def parse_product(self, response):

        #<h3 class="firm">Firmware</h3>
        if response.xpath('//h3[@class="firm"]').extract():
            for tr in response.xpath('//*[@id="tab-downloads"]/table[1]/tbody/tr'):
                print(tr.extract())
                url = tr.xpath("./td[2]/a/@href").extract()[0]
                date = tr.xpath("./td[4]/text()").extract()[0]
                version = tr.xpath("./td[5]/text()").extract()[0]
                description = tr.xpath("./td[7]/text()").extract()[0]
                product = url.split('-')[0]

                item = FirmwareLoader(item=FirmwareItem(),
                                      response=response)

                item.add_value("version", version)
                item.add_value("description", description)
                item.add_value("file_urls", "https://www.buffalotech.com/products/airstation-highpower-n300-open-source-dd-wrt-wireless-router")
                item.add_value("product", product)
                item.add_value("vendor", self.name)
                yield item.load_item()
