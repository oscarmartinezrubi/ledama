################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama import utils
from ledama.leddb.Naming import *
from ledama.leddb.query.QueryManager import QueryManager

class GetGainMovies(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('ldsbids','b','LDSB IDs', mandatory=False)
        options.add('ldsbpids','p','LDSBP IDs', mandatory=False)
        options.add('msids','m','MS IDs', mandatory=False)
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('timeout','t','Timeout', helpmessage=' for DB queries (-1 means no timeout)', default=60)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)    
        # the information
        information = 'Get the Gain Movies. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   

    def process(self, ldss, ldsbids, ldsbpids, msids, full, timeout, dbname,dbuser,dbhost):
        qm = QueryManager()
        queryOption = GAINMOVIE_KEY
        names = None # you can find in QueryTables all the possible column names
        if not full: 
            # The first column (identifier)
            names = [GAINMOVIE+ID,]
            
        qc = qm.initConditions()
        if ldss != "":
            qm.addCondition(queryOption, qc, LDS, tuple(ldss.split(',')))
        if ldsbids != "":
            qm.addCondition(queryOption, qc, LDSB+ID, tuple(utils.getElements(ldsbids)))
        if ldsbpids != "":
            qm.addCondition(queryOption, qc, LDSBP+ID, tuple(utils.getElements(ldsbpids)))
        if msids != "":
            qm.addCondition(queryOption, qc, MS+ID, tuple(utils.getElements(msids)))
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)          
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        if full:
            print qm.executeQuery(connection, cursor, query, queryDict, True)
        qm.executeQuery(connection, cursor, query, queryDict)
        qm.fetchAndShow(queryOption, names, connection, cursor, not full)
        
        cursor.close()
        connection.close()        