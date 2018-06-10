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
import pandas as pd  #用来读MySQL
import redis
redis_db = redis.Redis(host='127.0.0.1', port=6379, db=4) #连接redis，相当于MySQL的conn
redis_data_dict = "f_url"  #key的名字，写什么都可以，这里的key相当于字典名称，而不是key值。



import os
# import MySQLdb  #MySQL数据库2.7
# import MySQLdb.cursors
import pymysql  #3.5版本
import logging  #日志

# from twisted.enterprise import adbapi

from scrapy import signals
from openpyxl import Workbook

from scrapy.exceptions import DropItem   #用于item不符合要求时，提供报错信息


#保存到数据库+redis
class DuplicatesPipeline(object):
    wb = Workbook()  # 创建工作簿,同时页建一个sheet
    ws = wb.active
    ws.append(['主键', '职位名称', '详情链接', '公司名称', '薪资(千/月)', '更新时间', '薪资范围', '招聘人数', '父链接'])  # 设置表头

    def __init__(self):
        self.connect = pymysql.connect('127.0.0.1', db='qcwy', port=3306, user='root', passwd='system', charset='utf8')
        self.cursor = self.connect.cursor()
        sql = '''
                CREATE TABLE if not exists wh_table (
                                  id int not null auto_increment, 
                                  title VARCHAR(100), 
                                  link VARCHAR(200), 
                                  company VARCHAR(100), 
                                  salary  VARCHAR(20),
                                  updatetime VARCHAR(20), 
                                  salary_range VARCHAR(30), 
                                  num VARCHAR(10),
                                  parent_link VARCHAR(200),
                                  primary key(id)
                                 )DEFAULT CHARSET=utf8;
                    '''
        self.cursor.execute(sql)

        redis_db.flushdb() #删除全部key，保证key为0，不然多次运行时候hlen不等于0，刚开始这里调试的时候经常出错。
        if redis_db.hlen(redis_data_dict) == 0:
            sql = "SELECT link FROM wh_table;"
            df = pd.read_sql(sql, self.connect) #读MySQL数据
            for url in df['link'].get_values(): #把每一条的值写入key的字段里
                redis_db.hset(redis_data_dict, url, 0) #把key字段的值都设为0，你要设成什么都可以，因为后面对比的是字段，而不是值。


    def process_item(self, item, spider):
        #得到salary字段
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


        if redis_db.hexists(redis_data_dict, item['link']):  # 取item里的url和key里的字段对比，看是否存在，存在就丢掉这个item。不存在返回item给后面的函数处理
            raise DropItem("Duplicate item found: %s" % item)
        values = [item['title'], item['link'], item['company'], item['salary'], item['updatetime'], item['salary_range'], item['num'], item['parent_link']]
        self.do_insert(values)
        self.write(item,'武汉IT招聘信息统计.xlsx')
        self.connect.commit()
        return item

    #保存到数据库
    def do_insert(self, values):
        sql = 'insert into wh_table ( title , link , company ,  salary , updatetime , salary_range, num, parent_link)  values( %s, %s, %s, %s, %s, %s, %s, %s)'
        self.cursor.execute(sql, values)


    def close_spider(self, spider):
        self.connect.close()

    #写入xlsx文件
    def  write(self,item,filename):
        line = [item['id'], item['title'], item['link'], item['company'], item['salary'], item['updatetime'],
                item['salary_range'], item['num'], item['parent_link']]  # 把数据中每一项整理出来
        self.ws.append(line)  # 将数据以行的形式添加到xlsx中
        if os.path.exists('./result/51job'):
            fname = './result/51job/'+filename
            self.wb.save(fname)  # 保存xlsx文件
        else:
            print('不存在result下的51job路径')







#结果保存到xls中
class QcwyJsonPipeline(object):
    wb = Workbook()  #创建工作簿,同时页建一个sheet
    ws = wb.active
    ws.append(['主键', '职位名称', '详情链接', '公司名称', '薪资(千/月)', '更新时间', '薪资范围','招聘人数','父链接'])  # 设置表头


    def process_item(self, item, spider):  # 工序具体内容

        salary_tmp = item['salary'].decode('utf-8')   #去除千/月的后缀，只保留数字；统一将薪资设置成"千/月"
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

        line = [item['id'], item['title'], item['link'], item['company'], item['salary'], item['updatetime'],
                item['salary_range'],item['num'],item['parent_link']]  # 把数据中每一项整理出来
        self.ws.append(line)  # 将数据以行的形式添加到xlsx中
        self.wb.save('51_wh.xlsx')  # 保存xlsx文件

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



#结果保存到数据库中
class QcwyMySQLPipeline(object):
    """docstring for MySQLPipeline"""

    def __init__(self):
        self.client = pymysql.connect(
                                              host='127.0.0.1',
                                              db='qcwy',
                                              port = 3306,
                                              user='root',
                                              passwd='system',
                                              charset='utf8'
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

        sql = '''
        CREATE TABLE if not exists sz_table2 (
                          id int not null auto_increment, 
                          title VARCHAR(100), 
                          link VARCHAR(200), 
                          company VARCHAR(100), 
                          salary  VARCHAR(20),
                          updatetime VARCHAR(20), 
                          salary_range VARCHAR(30), 
                          num VARCHAR(10),
                          parent_link VARCHAR(200),
                          primary key(id)
                         )DEFAULT CHARSET=utf8;
            '''
        self.cur.execute(sql)

        self.cur.execute("insert into sz_table2 ( title , link , company ,  salary , updatetime , salary_range, num, parent_link)  values( %s, %s, %s, %s, %s, %s, %s, %s)",
                    ( item['title'], item['link'], item['company'], item['salary'], item['updatetime'], item['salary_range'], item['num'], item['parent_link']))
        self.client.commit()



    def handle_error(self, e):
        logging.error(e)
