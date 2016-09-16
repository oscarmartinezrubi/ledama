import cherrypy
from cherrypy.lib import httpauth 
import os,json
from ledama import utils
from ledama.leddb.Naming import *
from ledama.leddb.Connector import Connector
from ledama.ReferenceFile import ReferenceFile
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.query.QueryConditions import QueryConditions

TIMEOUT = 15

class GetRefFile:
    @cherrypy.expose
    def default(self, **args):
        jDict = {'status' : 'error','msg': 'File already exists'}
        ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
        username = ah['username']
        # Get data from fields
        queryOption = args["qo"]
        selectedValues = args["sv[]"]
        if type(selectedValues) != type([]):
            selectedValues = [selectedValues,]
        fname = args["fname"]

        user_dir = os.path.join('users', username)
        try:#to create a directory for the user.
            os.mkdir(user_dir)
        except:
            #assume dir already exists.
            pass
        files = os.listdir(user_dir)
        if fname in files:
            #file already exists warn the user.
            return json.dumps(jDict)
        else:
            fname = os.path.join(user_dir, fname) #set the file path to userdir

        qm = QueryManager()
        # The query parameters for the query to do
        qc = qm.initConditions()
        lqps = cherrypy.session.get('lastqueryparams')
        ignoreSelected = False
        usePrevious = (selectedValues != None) and (len(selectedValues) > 0)
        if usePrevious and lqps != None:
            lqp = lqps.get(queryOption)
            if lqp != None:
                (lastQCdata, lastNumRows) = lqp
                numCurrentSelected = len(selectedValues)
                if numCurrentSelected > 10 and (numCurrentSelected == lastNumRows):
                    ignoreSelected = True
                    # Instead of selected values in previous tab, we use the several query parameters used then (only if all the reows were selected)
                    qm.updateConditions(qc, QueryConditions(lastQCdata))   
                    
        if not ignoreSelected and selectedValues != None and len(selectedValues):
            if queryOption == LDS_KEY:
                tableId = NAME
            else:
                tableId = qm.getQueryTableId(queryOption)
            qm.addCondition(queryOption, qc, tableId, tuple(selectedValues))

        
        names = [PARENTPATH, NAME, SIZE, HOST, BEAMINDEX, CENTFREQ ]
        (query, queryDict) = qm.getQuery(MSP_KEY, qc, names)
            
            
        connection = Connector(dbuser = utils.getUserName()).getConnection()
        cursor = connection.cursor()
        queryStatement = qm.executeQuery(connection, cursor, query, queryDict, True)

        try:
            qm.executeQuery(connection, cursor, query, queryDict, False, TIMEOUT)
        except:
            jDict['msg'] = 'Connection timeout'
            try:
                cursor.close()
            except:
                pass
            connection.close()
            return json.dumps(jDict)
                
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
            centralFrequency = rowDict.get(CENTFREQ)
            if centralFrequency != None:
                centralFrequency = '%.03f' % float(centralFrequency)
            refFreqs.append(centralFrequency)

        cursor.close()
        connection.close()
        
        if len(absPaths) == 0:
            jDict['msg'] = 'No MSPs found in selection'
        else:     
            # Write the reference file
            referenceFile = ReferenceFile(fname, queryStatement, absPaths, refFreqs, sizes, nodes, beamIndexes)
            referenceFile.write() 
            jDict['status'] = 'success'
            jDict['msg'] = os.path.abspath(fname)
            jDict['link'] = fname
        return json.dumps(jDict) 
    
