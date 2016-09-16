################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import Connector, DEF_DBNAME, DEF_DBHOST
from ledama.leddb.Naming import *

class SizeLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('tables','t','Also check individual tables?',default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        
        # the information
        information = 'get the size of the LEDDB'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def process(self, tables, dbname, dbuser, dbhost):
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        # Get the cursor
        cursor = connection.cursor()
        cursor.execute("SELECT pg_size_pretty(pg_database_size('" + dbname + "'))")
        print 'Current ' + dbname + ' is ' + str(cursor.fetchone()[0])
        
        if tables:
            cursor.execute("SELECT tablename FROM pg_tables where schemaname='public' order by tablename")
            tableNames = cursor.fetchall()
            for (tableName,) in tableNames:
                cursor.execute("SELECT pg_size_pretty(pg_total_relation_size('" + tableName + "'))")
                print '    ' + str(cursor.fetchone()[0]) + '\t' + tableName 
            
        cursor.close()
        connection.close()
