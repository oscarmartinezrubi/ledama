################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama import utils
from ledama.leddb.query.QueryManager import QueryManager

class GetLDSBPs(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('ldsbids','i','LDSB IDs', mandatory=False)
        options.add('versions','v','Version indexes', mandatory=False)
        options.add('beams','b','Beams indexes', mandatory=False)
        options.add('stores','s','Stores', mandatory=False)
        options.add('part','p','Part row', mandatory=False, helpmessage=', it will print part of the information for each row', default=False)
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)              
        # the information
        information = 'Get the LDSBPs. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   
    
    def process(self, ldss, ldsbids, versions, beams, stores, part, full, dbname, dbuser, dbhost):
        qm = QueryManager()
        queryOption = LDSBP_KEY
        names = None # you can find in QueryTables all the possible column names
        if part:
            # We only give the most important columns
            names = [LDSBP+ID, LDS, STORE, DESCR, VERSION, BEAMINDEX, RAW, TAR, 
                     BVF, LDSBPNUMMSP, LDSBPTOTALSIZE, NUMCHAN, INTTIME]
        elif (not full):
            # The first column (identifier)
            names = [LDSBP+ID,]

        qc = qm.initConditions()
        if ldss != "":
            qm.addCondition(queryOption, qc, LDS, tuple(ldss.split(',')))
        if ldsbids != "":
            qm.addCondition(queryOption, qc, LDSB+ID, tuple(utils.getElements(ldsbids)))
        if versions != "":
            qm.addCondition(queryOption, qc, VERSION, tuple(utils.getElements(versions)))
        if beams != "":
            qm.addCondition(queryOption, qc, BEAMINDEX, tuple(utils.getElements(beams)))
        if stores != "":
            qm.addCondition(queryOption, qc, STORE, tuple(stores.split(',')))
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        if full or part:
            print qm.executeQuery(connection, cursor, query, queryDict, True)
        qm.executeQuery(connection, cursor, query, queryDict)
        qm.fetchAndShow(queryOption, names, connection, cursor, not (full or part))
        cursor.close()
        connection.close()        