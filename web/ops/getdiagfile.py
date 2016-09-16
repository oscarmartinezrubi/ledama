import os
import json
import cherrypy
from cherrypy.lib import httpauth 
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.query.QueryConditions import QueryConditions
from ledama.DiagnosticFile import DiagnosticFile

TIMEOUT = 60

class GetDiagFile:
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
            qm.addCondition(queryOption, qc, qm.getQueryTableId(queryOption), tuple(selectedValues))
            
        DiagnosticFile(fname, queryOption, qc)
        jDict['status'] = 'success'
        jDict['msg'] = os.path.abspath(fname)
        jDict['link'] = fname
        
        return json.dumps(jDict) 
