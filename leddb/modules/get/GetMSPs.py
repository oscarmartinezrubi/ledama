################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import utils
from ledama.ReferenceFile import ReferenceFile
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb.query.QueryManager import QueryManager

class GetMSPs(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldss','l','LDSs', mandatory=False)
        options.add('ldsbids','b','LDSB IDs', mandatory=False)
        options.add('ldsbpids','p','LDSBP IDs', mandatory=False)
        options.add('msids','m','MS IDs', mandatory=False)
        options.add('subbands','s','SB indexes', mandatory=False)
        options.add('nodes','n','Nodes', mandatory=False, helpmessage=', you can use the format as in rest of LModules. For example 1-10 means node001 to node010')
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('output','o','Output RefFile', mandatory=False)
        options.add('timeout','t','Timeout', helpmessage=' for DB queries (-1 means no timeout)', default=60)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)    
        # the information
        information = 'Get the MSPs (and optionally create a refFile). When specifying multiple values provide them with comma separation without white spaces. The logical operation applied in each option is OR'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   
    
    def process(self, ldss, ldsbids, ldsbpids, msids, subbands, nodes, full, output, timeout, dbname,dbuser,dbhost):
        qm = QueryManager()
        queryOption = MSP_KEY
        names = None
        if output != '':
            # If full we will ask only the columns required for the RefFile
            names = [MSP+ID, PARENTPATH, NAME, SIZE, HOST, BEAMINDEX, CENTFREQ ]
            # Check for already existing output files
            if os.path.isfile(output):
                print 'WARNING: ' + output + ' already exists. It will be overwritten...'
        elif (not full):
            # The first column (identifier)
            names = [MSP+ID,]

        qc = qm.initConditions()
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
        if nodes != "":
            qm.addCondition(queryOption, qc, HOST, tuple(utils.getNodes(nodes)))
        (query, queryDict) = qm.getQuery(queryOption, qc, names, formatcols = True)            
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        queryStatement = qm.executeQuery(connection, cursor, query, queryDict, True)
        if full or output != '':
            print queryStatement
        
        try:
            qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
        except:
            connection.close()
            print 'Error: Timeout connection'
            return
        
        mspIds = []
        if output != '':
            refFreqs=[]
            sizes=[]
            nodes=[]
            absPaths=[]
            beamIndexes=[]
            for row in cursor:
                rowDict = qm.rowToDict(row, names)
                absPaths.append(rowDict.get(PARENTPATH) + '/' + rowDict.get(NAME))
                sizes.append(rowDict.get(SIZE))
                nodes.append(rowDict.get(HOST))
                beamIndexes.append(rowDict.get(BEAMINDEX))
                refFreqs.append(rowDict.get(CENTFREQ))
                mspIds.append(str(rowDict.get(MSP+ID)))
            if len(absPaths) == 0:
                print 'No SBs found in node-paths.'
                return 
            
            # Write the reference file
            if os.path.isfile(output):
                os.system('rm ' + output)
            referenceFile = ReferenceFile(output, queryStatement, absPaths, refFreqs, sizes, nodes, beamIndexes)
            referenceFile.write()
            print ','.join(mspIds)
        else:
            qm.fetchAndShow(queryOption, names, connection, cursor, not full)
                
        cursor.close()
        connection.close()        