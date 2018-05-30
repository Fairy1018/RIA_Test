#!/usr/bin/env python
#coding=utf-8
import subprocess
print('启动爬虫程序')
dir = subprocess.getstatusoutput('scrapy crawl qcwysearch')
# print(dir)
if isinstance(dir[0],int) and dir[0] == 0:
	print('爬虫程序爬取数据成功！')
else:
	print('爬虫程序爬取数据失败！')

