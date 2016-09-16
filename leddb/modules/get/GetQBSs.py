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
from ledama.DiagnosticFile import DiagnosticFile
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb import LEDDBOps

class GetQBSs(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('qbsids','q','QBS IDs', mandatory=False)
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('ldsbids','i','LDSB IDs', mandatory=False)
        options.add('ldsbpids','p','LDSBP IDs', mandatory=False)
        options.add('msids','m','MS IDs', mandatory=False)
        options.add('subbands','b','SB indexes', mandatory=False)
        options.add('sbcf','c','SB central frequency range', mandatory=False, helpmessage=', specify [min]-[max] , for example, 120.0-150.5')
        options.add('qkindids','k','Quality Kinds', mandatory=False, helpmessage='. Use ? to see which options are available')
        options.add('baselineids','s','Baseline IDs', mandatory=False)
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('output','o','Output DiagFile', mandatory=False,helpmessage=', if specified the data is not queried, the query is stored in the DiagFile')
        options.add('timeout','t','Timeout', helpmessage=' for DB queries (-1 means no timeout)', default=60)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)                  
        # the information
        information = """Get the QBSs. When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR. 
We can use this LModule to obtain DiagFiles (with data or with only the query) to be used in the plotting LModules"""
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   

    def process(self, qbsids, ldss,ldsbids,ldsbpids,  msids, subbands, sbcf, qkindids, baselineids, full, output, timeout, dbname, dbuser, dbhost):
        qm = QueryManager()
        queryOption = QBS_KEY
        names = None
        if output != '':
            names  = qm.getQueryTable(queryOption).getNames(onlyinforvalues=True)
            # Check for already existing output files
            if os.path.isfile(output):
                print 'WARNING: ' + output + ' already exists. It will be overwritten...'
        elif (not full):
            # The first column (identifier)
            names = [QBSTAT+ID]
        
        qc = qm.initConditions()          
        if qbsids != "":
            qm.addCondition(queryOption, qc, QBSTAT+ID, tuple(utils.getElements(qbsids)))
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
        if baselineids != "":
            qm.addCondition(queryOption, qc, BASELINE+ID, tuple(utils.getElements(baselineids)))
        if sbcf != "":
            (minFreq,maxFreq) = utils.getFloats(sbcf)
            qm.addCondition(queryOption, qc, CENTFREQ, minFreq, '>')
            qm.addCondition(queryOption, qc, CENTFREQ, maxFreq, '<')
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if qkindids == '?':
            rows = LEDDBOps.select(connection, QKIND, columnNames = [QKIND + '.' + QKIND+ID,QKIND + '.' + NAME,])
            qKinds = []
            for [qKindId, qKindName] in rows:
                qKinds.append(str(qKindId) + ':' + qKindName)
            print 'There are ' + str(len(qKinds)) + ' available quality kinds: '
            print ','.join(sorted(qKinds))
            print 'Use the indexes to select them'
            return
        elif qkindids != "":
            qm.addCondition(queryOption, qc, QKIND+ID, tuple(utils.getElements(qkindids)))
        
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True) 
        
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