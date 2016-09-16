################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama import utils
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps
from ledama.leddb.query.QueryManager import QueryManager

class GetLDSs(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('projects','p','Projects', mandatory=False)
        options.add('field','t','Main field', mandatory=False, helpmessage=', specify only one field. Use POSIX regular expressions')
        options.add('beams','b','Num.Beams', mandatory=False)
        options.add('anttypes','a','AntennaTypes', mandatory=False)
        options.add('stations','s','Stations', mandatory=False, helpmessage=', shows LDS that used ALL the selected stations. Typing ? will print a list of stations used by the selected LDSs')
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)  
        # the information
        information = 'Get the LDSs. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR (except in stations wheret it is AND)'
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   

    def process(self, projects,field,beams,anttypes,stations,full,dbname,dbuser,dbhost):
        qm = QueryManager()
        queryOption = LDS_KEY
        names = None
        if not full: 
            # you can find n QueryTables all the possible column names (the first field)
            names = [NAME,]
        
        qc = qm.initConditions()
        if field != "":
            qm.addCondition(queryOption, qc, LDSMAINFIELD, field, '~')
        if projects != "":
            qm.addCondition(queryOption, qc, PROJECT, tuple(projects.split(',')))
        if anttypes != "":
            qm.addCondition(queryOption, qc, ANTTYPE, tuple(anttypes.split(',')))
        if beams != "":
            qm.addCondition(queryOption, qc, LDSNUMBEAMS, tuple(utils.getElements(beams)))
        if stations != "" and stations != '?':
            sts = tuple(stations.split(','))
            qm.addCondition(LDSHASSTATION_KEY, qc, STATION, sts)
            qm.addCondition(LDSHASSTATION_KEY, qc, NUMSELSTATIONS, len(sts))
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        if full:
            print qm.executeQuery(connection, cursor, query, queryDict, True)
        qm.executeQuery(connection, cursor, query, queryDict)
        ldss = qm.fetchAndShow(queryOption, names, connection, cursor, not full)
        
        if stations == '?':
            dataSelection  = None
            if len(ldss) < LEDDBOps.getTableNumRows(connection, LDSJOINED):
                dataSelection = {LDSHASSTATION + '.' + LDS: (tuple(ldss),'IN'),}
            stations = list(set(LEDDBOps.getColValues(LEDDBOps.select(connection, LDSHASSTATION, dataSelection, [LDSHASSTATION + '.' + STATION,]))))
            print 'There are ' + str(len(stations)) + ' stations that were used in at least one of the selected LDSs: '
            print ','.join(sorted(stations))
        cursor.close()
        connection.close()
