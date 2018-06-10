# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


# class QcwyPipeline(object):
#     def process_item(self, item, spider):
#         return item
#
'''
最新版qcwy
version 2.0
@Jason & Fairy
Python3.5
'''
import json
import codecs

# import MySQLdb  #MySQL数据库2.7
# import MySQLdb.cursors
import pymysql  #3.5版本
import logging  #日志

# from twisted.enterprise import adbapi

from scrapy import signals
from openpyxl import Workbook  #excel专用

from scrapy.exceptions import DropItem   #用于item不符合要求时，提供报错信息



class QcwyJsonPipeline(object):
    wb = Workbook()  #创建工作簿,同时页建一个sheet
    ws = wb.active
    ws.append(['主键', '职位名称', '详情链接', '公司名称', '薪资(千/月)', '更新时间', '薪资范围','招聘人数','父链接'])  # 设置表头


    def process_item(self, item, spider):  # 工序具体内容

        salary_tmp = item['salary']   #去除千/月的后缀，只保留数字；统一将薪资设置成"千/月"
        if salary_tmp.find(r'千/月') != -1:
            index = salary_tmp.find(r'千/月')
            tmp = salary_tmp[0:index]
            item['salary'] = tmp
        elif salary_tmp.find(r'万/月') != -1:
            index = salary_tmp.find(r'万/月')
            tmp = salary_tmp[0:index]
            salary_list = tmp.split('-')  #对“2-3”进行分割，转换成"千/月"
            if len(salary_list) == 2:
                salary_list[0] = float(salary_list[0]) * 10
                salary_list[1] = float(salary_list[1]) * 10
                result = str(salary_list[0]) + '-' + str(salary_list[1])
                item['salary'] = result
            else:
                raise DropItem("薪资获取不全，不符合‘5-6万/月’的格式 in %s" % item)
        elif salary_tmp.find(r'万/年') != -1:
            index = salary_tmp.find(r'万/年')
            tmp = salary_tmp[0:index]
            salary_list = tmp.split('-')  # 对“2-3”进行分割，转换成"千/月"
            if len(salary_list) == 2:
                salary_list[0] = round(float(salary_list[0]) / 12 * 10,2)  #round小数点之后保留两位
                salary_list[1] = round(float(salary_list[1]) / 12 * 10,2)
                result = str(salary_list[0]) + '-' + str(salary_list[1])
                item['salary'] = result
            else:
                raise DropItem("薪资获取不全，不符合‘5-6万/年’的格式 in %s" % item)
        else:
            raise DropItem("薪资格式不正确，不存在'千/月'、'万/月'、'万/年' in %s" % item)

        line = [item['key'], item['title'], item['link'], item['company'], item['salary'], item['updatetime'],
                item['salary_range'],item['num'],item['parent_link']]  # 把数据中每一项整理出来
        self.ws.append(line)  # 将数据以行的形式添加到xlsx中
        self.wb.save('./test1.xlsx')  # 保存xlsx文件
        return item

    # #旧版的process_item函数
    # def process_item(self, item, spider):  # 工序具体内容
    #     result = mode.findall(item['num'])
    #     if len(result) != 0:  # 匹配到数字
    #         item['num'] = int(result[0])  # 字符串转成数字
    #         line = [item['key'], item['title'], item['link'], item['company'], item['salary'], item['updatetime'],
    #                 item['salary_range'], item['num'], item['parent_link']]  # 把数据中每一项整理出来
    #         self.ws.append(line)  # 将数据以行的形式添加到xlsx中
    #         self.wb.save('./test_ref.xlsx')  # 保存xlsx文件
    #         return item

    # def spider_closed(self, spider):
    #     # self.file.close()
    #     print('爬虫程序结束')


class QcwyMySQLPipeline(object):
    """docstring for MySQLPipeline"""

    def __init__(self):
        self.client = pymysql.connect(
                                              host='127.0.0.1',
                                              db='qcwysearchdb',
                                              port = 3306,
                                              user='root',
                                              passwd='123456',
                                              charset='utf8',
                                              )
        self.cur = self.client.cursor()

    def process_item(self, item, spider):

        salary_tmp = item['salary'].decode('utf-8')  # 去除千/月的后缀，只保留数字；统一将薪资设置成"千/月"
        if salary_tmp.find(r'千/月') != -1:
            index = salary_tmp.find(r'千/月')
            tmp = salary_tmp[0:index]
            item['salary'] = tmp
        elif salary_tmp.find(r'万/月') != -1:
            index = salary_tmp.find(r'万/月')
            tmp = salary_tmp[0:index]
            salary_list = tmp.split('-')  # 对“2-3”进行分割，转换成"千/月"
            if len(salary_list) == 2:
                salary_list[0] = float(salary_list[0]) * 10
                salary_list[1] = float(salary_list[1]) * 10
                result = str(salary_list[0]) + '-' + str(salary_list[1])
                item['salary'] = result
            else:
                raise DropItem("薪资获取不全，不符合‘5-6万/月’的格式 in %s" % item)
        elif salary_tmp.find(r'万/年') != -1:
            index = salary_tmp.find(r'万/年')
            tmp = salary_tmp[0:index]
            salary_list = tmp.split('-')  # 对“2-3”进行分割，转换成"千/月"
            if len(salary_list) == 2:
                salary_list[0] = round(float(salary_list[0]) / 12 * 10, 2)  # round小数点之后保留两位
                salary_list[1] = round(float(salary_list[1]) / 12 * 10, 2)
                result = str(salary_list[0]) + '-' + str(salary_list[1])
                item['salary'] = result
            else:
                raise DropItem("薪资获取不全，不符合‘5-6万/年’的格式 in %s" % item)
        else:
            raise DropItem("薪资格式不正确，不存在'千/月'、'万/月'、'万/年' in %s" % item)

        if item.get('key'):
            #如果不存在数据库实例的话，就创建
            self.cur.execute("create database  if not exists qcwysearchdb_new;")
            self.cur.execute("use qcwysearchdb_new;")

            # self.cur.execute("drop table if exists `qcwysearch_py3_new`;")

            #如果已经存在`qcwysearch_py3_new`表，就不需要创建
            sql = '''
            CREATE TABLE if not exists `qcwysearch_py3_new` (
                                            `key` VARCHAR(100) NOT NULL PRIMARY KEY, #主键
                                            `title` VARCHAR(100), # 职位名称
                                            `link` VARCHAR(200), # 详情链接
                                            `company` VARCHAR(100), # 公司
                                            `salary` VARCHAR(20), # 薪资
                                            `updatetime` VARCHAR(20), # 更新时间
                                            `salary_range` VARCHAR(30), # 薪资范围
                                            `num` VARCHAR(10), # 招聘人数
                                            `parent_link` VARCHAR(200) # 上层链接
                                            )DEFAULT CHARSET=utf8;
            '''
            self.cur.execute(sql)

            self.cur.execute("insert into `qcwysearch_py3_new` (`key`, `title`, `link`, `company`, `salary`, `updatetime`, `salary_range`, `num`, `parent_link`) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                       (item['key'], item['title'], item['link'], item['company'], item['salary'], item['updatetime'], item['salary_range'], item['num'], item['parent_link']))
            self.client.commit()
            # self.cur.close()
            # self.client.close()

    # def spider_closed(self, spider):
    #     # self.cur.close()
    #     # self.client.close()
    #     print('爬虫程序结束！')

    def handle_error(self, e):
        logging.error(e)
