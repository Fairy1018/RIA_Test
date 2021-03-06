# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
'''
最新版qcwy
version 2.0
@Jason & Fairy
'''
from scrapy.item import Item, Field



# class QcwyItem(scrapy.Item):
#     # define the fields for your item here like:
#     # name = scrapy.Field()
#     pass




class QcwyItem(Item):
    # 定义要抓取信息的Item结构
    id = Field()   #数据库的主键  使用parent_link +随机数
    title = Field()  # 职位名称
    link = Field()  # 详情链接
    company = Field()  # 公司名称
    salary = Field()  #薪资
    updatetime = Field()  # 更新时间
    salary_range = Field()  #薪资范围
    num = Field()  #招聘人数
    parent_link = Field() #上层的链接(父链接),是一个组合的字符串，例如：%2520/180200/09/2107


