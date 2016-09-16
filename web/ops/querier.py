import json
import cherrypy
#imports for the pool
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from ledama import utils
from ledama import config as lconfig
from ledama.leddb.Naming import LDS, ID, LDSB, LDSBP, MS, MSP, GAIN, QTSTAT, QFSTAT, QBSTAT, GAINMOVIE, BASELINE, QKIND, STATION, GAIN_KEY, LDSHASSTATION_KEY,NUMSELSTATIONS
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.query.QueryConditions import QueryConditions

TIMEOUT = 20
TIMEOUT_DIAG = 60
MAXCONNECTIONS = 5

class Querier:
    dbUser = utils.getUserName()
    connectionpool = ThreadedConnectionPool(0, MAXCONNECTIONS, "dbname='" + DEF_DBNAME + "' user='" + dbUser + "' host='" + DEF_DBHOST + "'")
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def default(self, **args):
        dictQO = cherrypy.request.json
        # Get data from fields
        queryOption = dictQO['qo'] 
        previousQueryOption = dictQO['pqo'] 
        selectedValues = dictQO['sv']
        queryHeader = dictQO.get('header')
        tableOffset = dictQO['offset']
        tableLimit = dictQO['limit']
        qSO = dictQO['qso']
        extras = dictQO.get('extras')
        orderby = dictQO.get('orderby')
        
        # The object that we wwill return
        jDict = {'status' : 'error','msg': ''}
        
        # The query manager that will help with the data querying
        qm = QueryManager()
        # The queryTablecontains information on the table we want to query
        queryTable = qm.getQueryTable(queryOption)
        
        # For current queryTable we find the columns that could be filtered.
        # We exclude the ones that are already in SELECTION_OPTIONS (project, etc...) or as selected values (LDS, LDSBID...)
        filterables = queryTable.getNames(onlyfilterable=True)
        popNames = list(qm.getQuerySelectionOptions())
        popNames.extend([LDS, LDSB+ID, LDSBP+ID, MS+ID, MSP+ID, GAIN+ID, QTSTAT+ID, QFSTAT+ID, QBSTAT+ID, GAINMOVIE+ID, BASELINE+ID, QKIND+ID])
        for filterable in list(filterables):
            if filterable in popNames:
                filterables.pop(filterables.index(filterable))
        filterableheaders = queryTable.getHeaders(filterables)
        
        # This variable is indicating if there are problems while getting the queery objects
        fatalError = False
        # The query parameters for the query to do
        qc = qm.initConditions()
        
        # Now there are several steps in order to updte the conditions
        
        # STEP 1 - Use the SELECTED VALUES in previous tabs (or if we selected all of them
        # we just traspass the previous query conditions to current conditions)
        lqps = cherrypy.session.get('lastqueryparams')
        if lqps == None:
            lqps = {}
        ignoreSelected = False
        usePrevious = (selectedValues != None) and (len(selectedValues) > 0)
        if usePrevious:
            lqp = lqps.get(previousQueryOption)
            if lqp != None:
                (lastQCdata, lastNumRows) = lqp
                numCurrentSelected = len(selectedValues)
                if numCurrentSelected > 10 and (numCurrentSelected == lastNumRows):
                    ignoreSelected = True
                    # Instead of selected values in previous tab, we use the several query parameters used then (only if all the reows were selected)
                    qm.updateConditions(qc, QueryConditions(lastQCdata))        
        if not ignoreSelected and selectedValues != None and len(selectedValues):
            # Use previous selected stuff
            qm.addCondition(queryOption, qc, qm.getQueryTableId(previousQueryOption), tuple(selectedValues))
        
        # STEP 2 - Add the several column conditions (lower part of Filter window)
        if extras != None and len(extras) > 0:
            for extra in extras:
                eheader = extra['key']
                if eheader not in filterableheaders:
                    jDict['msg'] = 'Error in column filters. The column ' + eheader + ' does not exist or it is not filterable'
                    fatalError = True
                    break
                try:
                    value = qm.parseValues(extra['val'].strip())
                except Exception, e:
                    jDict['msg'] = 'Error parsing ' + eheader + ': ' + str(e)
                    fatalError = True
                    break
                qm.addCondition(queryOption, qc, queryTable.headersToNames([eheader,])[0], value, extra['op'])
                
        # STEP 3 - Add conditions on the QUERY SELECTION OPTIONS (upper part of Filter window), 
        # i.e. this conditions will be shared in all the tabs!! 
        if qSO != None and not fatalError:
            for selectionOption in qm.getEnabledSelectionOptions(queryOption)[0]:
                if selectionOption in qSO.keys():
                    qSODict = qSO[selectionOption]
                    (sel,num) = (qSODict['sv'], qSODict['numrows'])
                    numSel = len(sel)
                    if (numSel > 0) and (numSel != num):
                        name = selectionOption
                        added = False
                        if selectionOption in (BASELINE, QKIND):
                            name = selectionOption+ID
                        elif selectionOption == STATION:
                            if queryOption != GAIN_KEY:
                                qm.addCondition(LDSHASSTATION_KEY, qc, NUMSELSTATIONS, numSel, 'IN',removeifhas=True)                       
                                qm.addCondition(LDSHASSTATION_KEY, qc, name, tuple(sel), 'IN',removeifhas=True)
                                added = True
                        if not added:
                            qm.addCondition(queryOption, qc, name, tuple(sel), 'IN',removeifhas=True)
                            
        # STEP 4 - Define the order that will be applied in the query (ORDER BY statement) 
        orderbydesc = False # default ordering is ascending
        if orderby != None:
            orderby = queryTable.headersToNames([orderby,])
            lastorderby = cherrypy.session.get('orderby')
            lastorderbydesc = cherrypy.session.get('orderbydesc')
            if lastorderby != None and lastorderby == orderby:
                # switch the ordering
                orderbydesc = not lastorderbydesc
        else:
            if queryOption == cherrypy.session.get('qo'):
                # If we are still in the same query option we reuse the orderby
                orderby = cherrypy.session.get('orderby')
        
        # STEP 5 - Get the columns that we want to query (the ones on select statement)
        names = queryTable.headersToNames(queryHeader)
        
        # STEP 6 - Use the query manager to get the query to be exceuted
        (query, queryDict) = qm.getQuery(queryOption, qc, names, orderby, orderbydesc, formatcols = True)
        
        # STEP 7 - Get a connection/cursor from the pool or from session 
        connpool = cherrypy.session.get('connectionpool', self.connectionpool)
        connection = cherrypy.session.get('connection')
        if connection == None:
            connection = connpool.getconn(cherrypy.session.id)
        # Get a new cursor or from session
        cursor = cherrypy.session.get('cursor')
        if cursor == None:
            cursor = connection.cursor()
        # Define timeout (only in diagnostic queries)
        if qm.isDiagQuery(queryOption):
            timeout = TIMEOUT_DIAG
        else:
            timeout = TIMEOUT
        
        # STEP 8 - Execute the query (maybe it is not necessary because we are only scrolling...)
        timeoutActivated = False
        queryStatement = qm.executeQuery(connection, cursor, query, queryDict, True)
        if lconfig.DEBUG:
            print queryStatement
        if not fatalError and (tableOffset == 0 or ((tableOffset != 0) and tableOffset < cursor.rownumber) or not (cursor.rowcount > 0)):
            # We are in a new query    
            try:
                cursor.close()
                connection.reset()
                cursor = connection.cursor()
                qm.executeQuery(connection, cursor, query, queryDict, False, timeout)
            except Exception, e:
                timeoutActivated = True
                jDict['msg'] = str(e)
                # Remove timeout cursor from session 
                cherrypy.session.pop('cursor')
        
        # STEP 9 - Get the results (or scroll previous results)
        persist = False
        initrow = 0
        endrow = 0
        rows = []        
        if (not timeoutActivated) and (not fatalError):
            
            if (cursor.rowcount > 0):
                if tableOffset > cursor.rownumber:
                    if (tableOffset < cursor.rowcount):
                        cursor.scroll(tableOffset-cursor.rownumber)
                    else:
                        #go to last page
                        cursor.scroll((tableLimit * int(cursor.rowcount/tableLimit))-cursor.rownumber)
                initrow = cursor.rownumber
                rows = cursor.fetchmany(tableLimit)
                endrow = cursor.rownumber - 1
                if cursor.rownumber != cursor.rowcount:
                    persist = True
                jDict['status'] = 'success'
            else:
                jDict['msg'] = 'None rows returned for current query'

        # Define the rest of parameters that will be sent to the client
        jDict["header"] = queryHeader
        jDict["extras"] = filterableheaders
        jDict["headerinfo"] = queryTable.getInformations(names)
        jDict["maxrows"] = cursor.rowcount
        jDict["initrow"] = initrow
        jDict["endrow"] = endrow
        jDict["data"] = rows
        (enabledSO,enabledSO_F) = qm.getEnabledSelectionOptions(queryOption)
        jDict["qso"] = enabledSO
        jDict["qsof"] = enabledSO_F
        
        # Save SESSION information
        cherrypy.session['qo'] = queryOption
        # Save info of the orderby
        cherrypy.session['orderby'] = orderby
        cherrypy.session['orderbydesc'] = orderbydesc
        # Save info of the query parameters for the current tab
        lqps[queryOption] = (dict(qc.data),len(rows))
        cherrypy.session['lastqueryparams'] = lqps 
        if not persist:
            cursor.close()
            connection.cancel()
            connpool.putconn(connection)
            if cherrypy.session.has_key('cursor'): 
                cherrypy.session.pop('cursor')
            if cherrypy.session.has_key('connection'):
                cherrypy.session.pop('connection')
            if cherrypy.session.has_key('connectionpool'):
                cherrypy.session.pop('connectionpool')
        else:
            cherrypy.session['cursor'] = cursor
            cherrypy.session['connection'] = connection
            cherrypy.session['connectionpool'] = connpool
            
        return json.dumps(jDict)
