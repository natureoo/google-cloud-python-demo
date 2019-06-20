#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import time

# 打开数据库连接
db = MySQLdb.connect("localhost", "root", "12345678", "sample_db", charset='utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()

for i in range(30):
   # SQL 插入语句
   sql = "INSERT INTO SAMPLE_TABLE_SOURCE_INC(STATECODE, COUNTY)  VALUES ('data-{}', 'data-{}')".format(i ,i)
   try:
      # 执行sql语句
      cursor.execute(sql)
      # 提交到数据库执行
      db.commit()
      print("insert one record sleep 1 sec")
      time.sleep(1)
   except Exception as e:
      # Rollback in case there is any error
      db.rollback()
      print(e)

# 关闭数据库连接
db.close()