################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama.DiagnosticFile import DiagnosticFile
from ledama.leddb.query.QueryManager import QueryManager

class GetGains(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('gainids','g','Gain IDs', mandatory=False)
        options.add('stations','s','Stations', mandatory=False)
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('ldsbids','i','LDSB IDs', mandatory=False)
        options.add('ldsbpids','p','LDSBP IDs', mandatory=False)
        options.add('msids','m','MS IDs', mandatory=False)
        options.add('subbands','b','SB indexes', mandatory=False)
        options.add('sbcf','c','SB central frequency range', mandatory=False, helpmessage=', specify [min]-[max] , for example, 120.0-150.5')
        options.add('dirra','r','Direction RA range', mandatory=False, helpmessage=', specify [min]-[max]')
        options.add('dirdec','d','Direction DEC range', mandatory=False, helpmessage=', specify [min]-[max]')
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('output','o','Output DiagFile', mandatory=False,helpmessage=', if specified the data is not queried, the query is stored in the DiagFile')
        options.add('timeout','t','Timeout', helpmessage=' for DB queries (-1 means no timeout)', default=60)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)                
        # the information
        information = """Get the GAINs. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR. 
We can use this LModule to obtain DiagFiles (with data or with only the query) to be used in the plotting LModules"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   

    def process(self, gainids, stations, ldss, ldsbids, ldsbpids, msids, subbands, sbcf, dirra, dirdec, full, output, timeout, dbname, dbuser, dbhost):
        qm = QueryManager()
        queryOption = GAIN_KEY
        names = None
        if output != '':
            names  = qm.getQueryTable(queryOption).getNames(onlyinforvalues=True)
            # Check for already existing output files
            if os.path.isfile(output):
                print 'WARNING: ' + output + ' already exists. It will be overwritten...'
        elif (not full):
            # The first column (identifier)
            names = [GAIN+ID]
        
        qc = qm.initConditions()          
        if gainids != "":
            qm.addCondition(queryOption, qc, GAIN+ID, tuple(utils.getElements(gainids)))
        if stations != "":
            qm.addCondition(queryOption, qc, STATION, tuple(stations.split(',')))
        if ldss != "":
            qm.addCondition(queryOption, qc, LDS, tuple(ldss.split(',')))
        if ldsbids != "":
            qm.addCondition(queryOption, qc, LDSB+ID, tuple(utils.getElements(ldsbids)))
        if ldsbpids != "":
            qm.addCondition(queryOption, qc, LDSBP+ID, tuple(utils.getElements(ldsbpids)))
        if msids != "":
            qm.addCondition(queryOption, qc, MS+ID, tuple(utils.getElements(msids)))
        if subbands != "":
            qm.addCondition(queryOption, qc, SBINDEX, tuple(utils.getElements(subbands)))
        if sbcf != "":
            (minFreq,maxFreq) = utils.getFloats(sbcf)
            qm.addCondition(queryOption, qc, CENTFREQ, minFreq, '>')
            qm.addCondition(queryOption, qc, CENTFREQ, maxFreq, '<')
        if dirra != "":
            (minSDRA,maxSDRA) = utils.getFloats(dirra)
            qm.addCondition(queryOption, qc, DIRRA, minSDRA, '>')
            qm.addCondition(queryOption, qc, DIRRA, maxSDRA, '<')
        if dirdec != "":
            (minSDDec,maxSDDec) = utils.getFloats(dirdec)
            qm.addCondition(queryOption, qc, DIRDEC, minSDDec, '>')
            qm.addCondition(queryOption, qc, DIRDEC, maxSDDec, '<')
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        queryStatement = qm.executeQuery(connection, cursor, query, queryDict, True)
        if full or output != '':
            print queryStatement

        if output != '':
            if os.path.isfile(output):
                os.system('rm ' + output)
            DiagnosticFile(output, queryOption, qc)
        else:
            try:
                qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
                qm.fetchAndShow(queryOption, names, connection, cursor, not full)
            except:
                connection.close()
                print 'Error: Timeout connection'
                return
        cursor.close()
        connection.close()        