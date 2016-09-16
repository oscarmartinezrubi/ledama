#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
import cPickle as pickle
import ledama.utils as utils
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector

class DiagnosticFile:
    """ This dile is used to store the queryConditions of a query to LEDDB"""
    def __init__(self, filePath, queryOption = None, queryConditions = None):
        """ Initialize the diagnostic file. If queryOption and queryConditions are provided we assume we want to write a DiagFile. 
        If no queryOption neither queryConditions are provided we will attempt to read the DiagFile"""
        self.filePath = filePath
        
        if queryOption == None:
            self.__read()
        else:
            self.queryOption = queryOption
            self.queryConditions = queryConditions
            self.__write()
            
    def __read(self):
        # Open the file
        try:
            diagfile = open(self.filePath, "rb")
        except:
            raise Exception('Error: ' + self.filePath + ' not found')
        
        dataDict = pickle.load(diagfile)
        self.queryOption = dataDict.get('QUERYOPTION')
        self.queryConditions = dataDict.get('QUERYCONDITIONS')

    def __write(self):
        
        # Check for already existing output files
        if os.path.isfile(self.filePath):
            raise Exception('Error: ' + self.filePath + ' already exists')

        diagfile = open(self.filePath, "wb")
        # Protocol is binary!
        pickle.dump({"QUERYOPTION": self.queryOption ,"QUERYCONDITIONS": self.queryConditions}, diagfile, 2)
        diagfile.close()
        
    def queryData(self, names = None, orderBy = None, dbname = DEF_DBNAME, dbuser = "", dbhost = DEF_DBHOST, timeout = 60):
        if dbuser == '':
            dbuser = utils.getUserName()
        
        if self.queryOption == None or self.queryConditions == None:
            raise Exception('Error: query parameters were not found')
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
            
        qm = QueryManager()
        
        queryTable = qm.getQueryTable(self.queryOption)
        
        if names == None: # if not names is provided we use default onlyshown
            names = queryTable.getNames(onlyinforvalues=True)
        
        (query, queryDict) = qm.getQuery(self.queryOption, self.queryConditions, names, orderBy)
        cursor = connection.cursor()
        try:
            qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
        except:
            try:
                cursor.close()
            except:
                pass
            connection.close()
            raise Exception('Connection timeout')
        
        data = cursor.fetchall()         
        cursor.close()
        connection.close()
        return data
