# -*- coding: utf-8 -*-
from scrapy.item import Item, Field

# scraperにしたがってDjango形式でItemを書く
# default=Noneに意味はないです（Noneチェックはpipelines.pyで）

class FirmwareItem(Item):
    category = Field() # if you can't find category, use "Misc"
    vendor = Field()
    product = Field()

    description = Field(default=None)
    version = Field(default=None)
    date = Field(default=None)
    size = Field(default=None)
    language = Field(default=None)

    gpl = Field(default=None)
    url = Field()
    mib = Field(default=None)

    # Used by FilesPipeline
    file_urls = Field()
    files = Field()
