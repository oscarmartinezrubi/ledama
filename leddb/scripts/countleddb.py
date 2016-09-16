from ledama.leddb.Naming import ALLTABLES, QBSTAT, QFSTAT, QTSTAT, GAIN
from ledama.leddb.Connector import Connector
import optparse
import sys

opt = optparse.OptionParser()
opt.add_option('-w','--dbname',help='DB name',default ='')
opt.add_option('-y','--dbuser',help='DB user',default ='')
opt.add_option('-z','--dbhost',help='DB host',default ='')
options, arguments = opt.parse_args()

for option in (options.dbname,options.dbuser,options.dbhost):
    if option == '':
        print 'ERROR: SPECIFY ALL DB OPTIONS!'
        sys.exit(1)

connection = Connector(options.dbname,options.dbuser,options.dbhost).getConnection()
cursor = connection.cursor()

for table in ALLTABLES:
    if table not in (QBSTAT, QFSTAT, QTSTAT, GAIN):
        cursor.execute('select count(*) from ' + table)
        print table, cursor.fetchall()

for diagTable in (QBSTAT, QTSTAT, QFSTAT, GAIN):
    cursor.execute('select tablename from pg_tables where tablename ~ %s order by tablename', [diagTable.lower()+"_part_*",])
    partNames = cursor.fetchall()
    for (partName, ) in partNames:
        cursor.execute('select count(' + diagTable.lower() + '_id) from ' + partName)
        print partName, cursor.fetchall()
