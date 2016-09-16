#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from psycopg2 import connect
import ledama.utils as utils
import ledama.config as lconfig

DEF_DBNAME = lconfig.LEDDB_NAME
DEF_DBHOST = lconfig.LEDDB_HOST

# This class is used to connect to the LEDDB
class Connector:
    
    # Initialize the connection
    def __init__(self, dbname = DEF_DBNAME, dbuser = '', dbhost = DEF_DBHOST): 
        try:
            if dbuser == '':
                dbuser = utils.getUserName()
            self.connection = connect("dbname='" + dbname + "' user='" + dbuser + "' host='" + dbhost + "'")
        except Exception,e:
            print str(e)
            self.connection = None
         
    # Get the connection 
    def getConnection(self):    
        return self.connection
