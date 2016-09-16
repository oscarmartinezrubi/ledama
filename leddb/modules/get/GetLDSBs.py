################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama import utils
from ledama.PrettyTable import PrettyTable
from ledama.leddb.query.QueryManager import QueryManager

class GetLDSBs(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('beams','b','Beams indexes', mandatory=False)
        options.add('fields','t','Fields', mandatory=False)
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)       
        # the information
        information = 'Get the LDSBs. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR'
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   

    def process(self, ldss,beams,fields,full,dbname,dbuser,dbhost):
        qm = QueryManager()
        queryOption = LDSB_KEY
        names = None # you can find in QueryTables all the possible column names
        if not full: 
            # The first column (identifier)
            names = [LDSB+ID,]
            
        qc = qm.initConditions()
        if ldss != "":
            qm.addCondition(queryOption, qc, LDS, tuple(ldss.split(',')))
        if beams != "":
            qm.addCondition(queryOption, qc, BEAMINDEX, tuple(utils.getElements(beams)))
        if fields != "":
            qm.addCondition(queryOption, qc, FIELD, tuple(fields.split(',')))
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)
                
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        if full:
            print qm.executeQuery(connection, cursor, query, queryDict, True)
        qm.executeQuery(connection, cursor, query, queryDict)
        qm.fetchAndShow(queryOption, names, connection, cursor, not full)
        cursor.close()
        connection.close()        