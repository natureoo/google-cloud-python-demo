import MySQLdb

#库名：python;表名：students

conn = MySQLdb.connect(host='localhost',user='root',passwd='12345678',db='cloudtest',charset='utf8')
cursor = conn.cursor()
count = cursor.execute('select policyID from SAMPLE_TABLE_5 where policyID = 1')

mail_list=[]
#获取所有结果
results = cursor.fetchall()

for r in results:
    #print 'mail:%s ' % r
    mail_list.append('%d' % r )
# print(mail_list)
val = ','.join(mail_list)
print(val)

if val:
    #游标归零，默认mode='relative'
    cursor.scroll(0,mode='absolute')
    sql = "update cloudtest.SAMPLE_TABLE_5 set statecode = 'FLLL' where policyID in ({})".format(val)
    print(sql)
    count = cursor.execute(sql)
#
# print(count)

conn.close()