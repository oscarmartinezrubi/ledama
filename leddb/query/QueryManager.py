################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import threading
from ledama import utils
from ledama.leddb import LEDDBOps
from ledama.leddb.query import QueryTables
from ledama.leddb.Naming import *
from ledama.leddb.query.QueryConditions import QueryConditions
from ledama.leddb.query import queryoperations
from ledama.PrettyTable import PrettyTable

MAX_COL_CONDITIONS = 5

class QueryManager:
    """ This is the LEDDB Query manager. It handles the different queries to the LEDDB """
    def __init__(self, ):
        """ Initialize the LEDDB query manager. It defined the main query options (the queriable tabs)
        and the selection options which can add query conditions to the main tables queries (and those are related to a secondary table)"""
        self.QUERY_OPTIONS = [LDS_KEY,LDSB_KEY,LDSBP_KEY,MS_KEY,MSP_KEY,GAIN_KEY,QTS_KEY,QFS_KEY,QBS_KEY, GAINMOVIE_KEY]
        self.QUERY_SELECTION_OPTIONS = [PROJECT, FIELD, ANTTYPE, STATION, BASELINE, STORE, HOST, QKIND]
    
    def getQueryOptions(self):
        return self.QUERY_OPTIONS
    
    def getQuerySelectionOptions(self):
        return self.QUERY_SELECTION_OPTIONS
    
    def getQueryTable(self, queryOption):
        if (queryOption == LDS_KEY):
            return QueryTables.LDS_TABLE
        elif (queryOption == LDSB_KEY):
            return QueryTables.LDSB_TABLE
        elif (queryOption == LDSBP_KEY):
            return QueryTables.LDSBP_TABLE
        elif (queryOption == MS_KEY):
            return QueryTables.MS_TABLE
        elif (queryOption == MSP_KEY):
            return QueryTables.MSP_TABLE
        elif (queryOption == GAIN_KEY):                
            return QueryTables.GAIN_TABLE
        elif (queryOption == QTS_KEY):
            return QueryTables.QTS_TABLE
        elif (queryOption == QFS_KEY):
            return QueryTables.QFS_TABLE
        elif (queryOption == QBS_KEY):
            return QueryTables.QBS_TABLE
        elif (queryOption == GAINMOVIE_KEY):
            return QueryTables.GAIN_MOVIE_TABLE
        elif (queryOption == LDSHASSTATION_KEY):
            return QueryTables.LDSHASTATION_TABLE
        
    def getQueryTableId(self, queryOption):
        if queryOption == LDS_KEY:  
            return LDS # In this case it should be NAME, but we use LDS since that is the name in LDSB
        elif queryOption == LDSB_KEY:  
            return LDSB+ID
        elif queryOption == LDSBP_KEY:
            return LDSBP+ID
        elif queryOption == MS_KEY:
            return MS+ID
        elif queryOption == MSP_KEY:
            return MSP+ID
        elif queryOption == GAIN_KEY:
            return GAIN+ID
        elif queryOption == QTS_KEY:
            return QTSTAT+ID
        elif queryOption == QFS_KEY:
            return QFSTAT+ID
        elif queryOption == QBS_KEY:
            return QBSTAT+ID
        elif queryOption == GAINMOVIE_KEY:
            return GAINMOVIE+ID

    def isHigherQueryOption(self, qo1, qo2):
        if self.QUERY_OPTIONS.index(qo1) > self.QUERY_OPTIONS.index(qo2):
            return True
        return False
    
    def rowToDict(self, row, colsnames):
        """Parse a row into a dictionary, the keys are the colsnames values"""
        if len(row) != len(colsnames):
            raise Exception('ERROR: mismatch len(row)!=len(colsnames); (' + str(len(row)) + '!=' + str(len(colsnames)) + ') ')
        rDict = {}
        for i in range(len(row)):
            rDict[colsnames[i]] = row[i]
        return rDict
    
    # Get the several parameters of the querySelectionOption
    def getQuerySelectionOptionParameters(self, querySelectionOption, connection):
        if querySelectionOption not in self.QUERY_SELECTION_OPTIONS: 
            raise Exception('Error: ' + str(querySelectionOption) + ' unexpected query selection option. Select one of: ' + ','.join(self.QUERY_SELECTION_OPTIONS))
        if querySelectionOption == STATION:
            colNames = [NAME, ANTTYPE, LOCATIONTYPE]
            colHeader = ['station', 'antennaType', 'locationType']
            dataRows = LEDDBOps.select(connection, querySelectionOption, columnNames = colNames, orderBy = NAME)
            defaultIn = False
            helpText = 'Queried data will be related to all selected ' +querySelectionOption + 's, i.e. logic AND (except for ' + GAIN + ' where it is an OR)'
        elif querySelectionOption == BASELINE:
            colNames = [BASELINE+ID, STATION1, STATION2]
            colHeader = ['baselineId', 'station', 'station2']
            dataRows = LEDDBOps.select(connection, querySelectionOption, columnNames = colNames, orderBy = STATION1+','+STATION2)
            defaultIn = False
            helpText = 'Queried data will be related to one of the selected ' + querySelectionOption + 's, i.e. logic OR'
        elif querySelectionOption == QKIND:
            colNames = [QKIND+ID, NAME, DESCR]
            colHeader = ['QKId', 'QKName', 'description']
            dataRows = LEDDBOps.select(connection, querySelectionOption, columnNames = colNames, orderBy = QKIND+ID)
            defaultIn = False
            helpText = 'Queried data will be related to one of the selected ' + querySelectionOption + 's, i.e. logic OR'                
        else:
            #The rest of options are treated the same way
            colNames = [NAME,]
            colHeader = [querySelectionOption,]
            dataRows = LEDDBOps.select(connection, querySelectionOption, columnNames = colNames, orderBy = NAME)
            defaultIn = True
            helpText = 'Queried data will be related to one of the selected ' + querySelectionOption + 's, i.e. logic OR'
        return (colHeader,dataRows,defaultIn,helpText)
    
    def getEnabledSelectionOptions(self, queryOption):
        """ Get the selection options which are enabled for provided query option"""
        enabled = 0
        enabled_f = 1
        disabled = -1
        
        # [PROJECT, FIELD, ANTTYPE, STATION, BASELINE, STORE, HOST, QKIND]
        if (queryOption == LDS_KEY):
            enabledTypeSO = [enabled, enabled_f, enabled, enabled, enabled_f, enabled_f, enabled_f, enabled_f]
        elif (queryOption == LDSB_KEY):    
            enabledTypeSO = [enabled, enabled, enabled, enabled, enabled_f, enabled_f, enabled_f, enabled_f]
        elif (queryOption == LDSBP_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, enabled_f, enabled, enabled_f, enabled_f]
        elif (queryOption == MS_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, enabled_f, enabled, enabled_f, enabled_f]
        elif (queryOption == MSP_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, disabled, enabled, enabled, disabled]
        elif (queryOption == GAIN_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, disabled, enabled, disabled, disabled]
        elif (queryOption == QTS_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, disabled, enabled, disabled, enabled]
        elif (queryOption == QFS_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, disabled, enabled, disabled, enabled]
        elif (queryOption == QBS_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, enabled, enabled, disabled, enabled]
        elif (queryOption == GAINMOVIE_KEY):
            enabledTypeSO = [enabled, enabled, enabled, enabled, disabled, enabled, enabled, disabled]
        else:
            raise Exception('Error: unexpected query option (None?). Select and set one of: ' + ','.join(self.QUERY_OPTIONS))
        
        enabledQSO = []
        enabledQSO_f = []
        for i in range(len(self.QUERY_SELECTION_OPTIONS)):
            if enabledTypeSO[i] == enabled:
                enabledQSO.append(self.QUERY_SELECTION_OPTIONS[i])
            elif enabledTypeSO[i] == enabled_f:
                enabledQSO_f.append(self.QUERY_SELECTION_OPTIONS[i])
                
        return (enabledQSO,enabledQSO_f)
    
    def isDiagQuery(self, queryOption):
        return (queryOption == GAIN_KEY) or (queryOption == QTS_KEY) or (queryOption == QFS_KEY) or (queryOption == QBS_KEY)

    def parseValues(self, values):
        """ Parse a values string into useful types. Options:
            - [min]..[max]..[step] will generate a range of integers
            - [min]..[max] will generate a range of integers
            - single float, integers, strings or boolean (for a boolean we can use true, True, t or T (same with False))
            - lists of float, integers, strings or booleans (comma-separated)
            """
        vals = None
        if len(values.split('..')) in (3,2):
            #This should be a int range
            vals = utils.getElements(values)
        else:
            vals = values.split(',')
            for i in range(len(vals)):
                try:
                    vals[i] = float(vals[i])
                except:
                    try:
                        vals[i] = int(vals[i])
                    except:
                        bval = vals[i].strip()
                        if bval == 'true' or bval == 'True' or bval == 't'or bval == 'T':
                            vals[i] = True
                        elif bval == 'false' or bval == 'False' or bval == 'f'or bval == 'F':
                            vals[i] = False
        if len(vals) == 1:
            return vals[0]
        else:
            return tuple(vals)
                
    def initConditions(self):
        qc = QueryConditions()
        qc.data.clear()
        return qc
    
    def updateConditions(self, queryConditions1, queryConditions2):
        queryConditions1.update(queryConditions2)
    
    def addCondition(self, queryOption, queryConditions, name, value, operator='IN', counter=1, removeifhas=False):
        """ Add a condition in the queryConditions on a column"""
        # If there is already a condition related to this column we try to add a white space (it is recursive, so we can add several)
        if counter == MAX_COL_CONDITIONS:
            raise Exception('ERROR: Maximum number of conditions for same column is ' + MAX_COL_CONDITIONS)
        if type(value) == tuple:
            if operator == '=':
                operator = 'IN'    
            elif operator == '!=':
                operator = 'NOT IN'
            elif operator not in ('IN', 'NOT IN'):
                raise Exception('Error parsing condition. Multiple values only accept = or !=')
        queryTable = self.getQueryTable(queryOption)
        key = (queryTable.getTables(names = [name.strip(),])[0] , name)
        if queryConditions.has(key):
            if removeifhas:
                queryConditions.remove(key)
                queryConditions.add(queryTable, name, value, operator)        
            else:
                self.addCondition(queryOption, queryConditions, name+' ', value, operator, counter+1)
        else:
            queryConditions.add(queryTable, name, value, operator)
  
    def getQuery(self, queryOption, queryConditions, names = None, orderBy = None, desc = False, offset = None, limit = None, formatcols = False):
        """ It get the query objects for given queryOption and queryConditions. names is the queried columns in the queried table (queryOption)"""
        tables = [LDSHASSTATION, LDSJOINED]
        nctables = []
        distinct = False
        if self.isHigherQueryOption(queryOption, LDS_KEY):
            tables.append(LDSBJOINED)
        if self.isHigherQueryOption(queryOption, LDSB_KEY):
            tables.append(LDSBPJOINED)
        if self.isHigherQueryOption(queryOption, LDSBP_KEY):
            tables.append(MSJOINED)
        
        if(queryOption == MSP_KEY):
            tables.append(MSP)
        elif (queryOption == GAIN_KEY):
            tables.append(GAIN)                
        elif (queryOption == QTS_KEY):
            tables.append(QTSTAT)
            nctables = [QKIND,]
        elif (queryOption == QFS_KEY):
            tables.append(QFSTAT)
            nctables = [QKIND,]
        elif (queryOption == QBS_KEY):
            tables.append(QBSTAT)
            nctables = [QKIND,BASELINE]
        elif (queryOption == GAINMOVIE_KEY):
            tables.append(GAINMOVIEHASMS)
            tables.append(GAINMOVIE)
            distinct = True
        
        numtables = len(tables)
            
        # Get the query table for the current query option
        queryTable = self.getQueryTable(queryOption)
        if names == None: # if not names is provided we use default onlyshown
            names = queryTable.getNames(onlyshown=True)
        # Get a copy of the query conditions 
        # (we need it because in the getConditions function calls it gets modified)
        qc = queryConditions.copy()
        
        tableConditions = {}
        for i in range(numtables):
            # we use reverse order so the conditions that can be in two different 
            # tables are in the table with less hierarchy level, in this way we 
            # may avoid some unnnecessary joins
            table = tables[-(i+1)] 
            tableConditions[table] = queryoperations.getConditions(table, qc)
        # Get other tables that are required for queried columns or order by statement
        if orderBy == None:
            orderBy = queryTable.getDefaultOrder()
        oTabs = queryTable.getTables(names + orderBy)
        # Initialize the from statement string
        fromStatement = queryoperations.initFromStatement()
        # We update the fromStatement taking into account possible conditions in
        # all tables with higher hierarchy  
        for i in range(numtables-1):    
            fromStatement = queryoperations.updateFromStatement(fromStatement, tables[i], tables[i+1], tableConditions[tables[i]], tableConditions[tables[i+1]], oTabs)
        # We update from statement from other tables from which we know there are not conditions 
        for nctable in nctables:
            fromStatement = queryoperations.updateFromStatement(fromStatement, tables[-1], nctable, tableConditions[tables[-1]], None, oTabs)
        
        # Finish the from statement string
        fromStatement = queryoperations.finishFromStatement(fromStatement, tables[-1], tableConditions[tables[-1]])
        
        query = queryoperations.getSelectQuery(queryTable, names, fromStatement, orderBy, offset, limit, desc, distinct, formatcols)
        queryDict = queryConditions.getValuesDict()
        
        return (query, queryDict)  
    
    
    def executeQuery(self, connection, cursor, query, queryDict, getqcommand=False, timeout=0):
        """ Executes a query in the given connection - cursor 
        (the rows are not returned, they must be fetch with cursor.fetch* methods or with self.fetch). 
        If timeout is specified, the query will be cancelled after timeout.
        If getqcommand is True the query is not executed. Instead a string with the query is returned"""
        if getqcommand:
            return cursor.mogrify(query, queryDict)
        if timeout > 0:
            t = threading.Timer(timeout,connection.cancel)
            t.start()
            cursor.execute(query, queryDict)
            t.cancel()
        else:
            # Not timeout
            cursor.execute(query, queryDict)
            
    def fetchAndShow(self, queryOption, names, connection, cursor, onlyids=False):
        """ Fetch the result of a query. If onlyids is True, only the ids are print,
        otherwise the table is shown (with the headers derived from names).
        The ids are returned"""
        queryTable = self.getQueryTable(queryOption)
        ids = []
        if onlyids:
            for row in cursor:
                ids.append(str(row[0]))
        else:
            if names == None: # if not names is provided we assume the default onlyshown
                names = queryTable.getNames(onlyshown=True)
            pTable = PrettyTable(queryTable.getHeaders(names))
            while cursor.rownumber < cursor.rowcount:
                for row in cursor.fetchmany(1000):
                    pTable.add_row(row)
                    ids.append(str(row[0]))
                print pTable.get_string()
            print 'There are ' + str(len(ids)) + ' ' + queryOption + 's matching the selection: ' 
        print ','.join(ids)
        return ids
        
