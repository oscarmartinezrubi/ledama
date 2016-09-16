#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import LDSBP, ID, INTTIME, NUMCHAN, DESCR,FLAGGED, \
AVERAGED ,CALIBRATED,DIRDEPCAL,DIRINDEPCAL, MS, SBINDEX, CENTFREQ, BW, STORE, \
LDSB, VERSION

# This python module is used to define operations of access to the LEDDB

def insert(connection, table, data, returning = None):
    """ Insert the data into the table. The parameters are:
            - connection: a valid connection instance
            - table: string with the name of the table. The table must be a valid LEDDB table
            - data: dictionary with the data to be added. The content must be columnname:value
            - returning is an array with list of column names we will like to return """
    columnNames = ''
    aux = ''
    for key in data.keys():
        columnNames += key + ','
        aux += '%s,'
    columnNames = columnNames[:-1]
    aux = aux[:-1]
    
    retColNames = ''
    if returning != None:
        retColNames = ' RETURNING '
        for retfield in returning:
            retColNames += retfield+','
        retColNames = retColNames[:-1]
    cursor = connection.cursor()
    rows = None
    try:
        # We try to insert it, this will fail if the data has been already
        # added by some other process in the meanwhile 
        cursor.execute('INSERT INTO ' + table + ' (' + columnNames + ') VALUES (' + aux + ')' + retColNames, data.values())
        connection.commit()
        if returning != None:
            rows = cursor.fetchall()
    except Exception,e:
        #print 'concurrent adding in ' + table + ' of ' + str(data.values())
        # Only the Integrity errors are accepted due to unique constraint
        if str(e).count('unique') == 0:
            connection.close()
            raise Exception('error in ' + table + ' table when adding row ' + str(data.values()) + ':\n' + str(e))
        connection.rollback()
    cursor.close()
    if rows != None:
        return rows

def select(connection, table, dataForSelection = None, columnNames = None, orderBy = None, limit = None, offset = None, toPrint = False, returnCursor = False, distinct = False):
    """ Select the columns indicated by columnNames from the table/s selecting by the dataForSelection. 
            - connection is a valid connecction instance
            - table is a string with a LEDDB table name. It can also be a list of LEDDB tables names. In this case remember that you need to add a condition in dataForSelection to properly join these tables
            - dataForSelection is a dictionary which keys are column names and values are the column value to search or a tuple. 
            If a tuple is provided, it must have two component. First one is the value/s and the second is the operator (default operator is =)
            In this case, the value can also be a tuple (for example if we want to use IN as operator)
            In case of multiple tables the columns must be indicated as TableName.ColumnName
            In case of multiple tables we also need to add the condition that will allow to make the JOIN (usually through table IDs)
            If user wants to put two conditions over the same column in the dataForSelection, add white spaces in the column name to differentiate them
            - columnNames is a list of names of the columns that we want to select.  
            In case of multiple tables the columns must be indicated as TableName.ColumnName
            It can also be None or an empty list in which case all the columns of all the queried tables are returned.
            - orderBy is a string that the column name in which we want to order the data. It can also be a list. It can be None
            - limit is a number (or None) to limit the number of returned rows
            - offset is to add an offset in the query
            - toPrint will indicate if the query will be printed to stdout
            - returnCurosr indicates if we want to return the obtained rows or the cursor (from which we get the rows) By default the rows are returned
            - distinct indicates if we want to add distinct statement in the query
 """
    
    if type(table) == list:
        table = ','.join(table)
    if columnNames == None or len(columnNames) == 0:
        selectStatement = '*'
    else:
        selectStatement = ','.join(columnNames)
        
    if distinct:
        selectStatement = ' DISTINCT ' + selectStatement
    
    orderByStatement = ''
    if orderBy != None:
        if type(orderBy) in (list,tuple):
            orderByStatement = ' ORDER BY ' + ','.join(orderBy)
        else:
            orderByStatement = ' ORDER BY ' + orderBy
        
    limitOffsetStatement = ''
    if offset != None:
        limitOffsetStatement = ' OFFSET ' + str(offset)
    if limit != None:
        limitOffsetStatement += ' LIMIT ' + str(limit)
        
    cursor = connection.cursor()
    if dataForSelection != None and len(dataForSelection):
        condition = ''
        values = []
        for key in dataForSelection.keys():
            dataForSelectionValue = dataForSelection[key]
            if key.strip() == '':
                if type(dataForSelectionValue) == tuple:
                    # This is to add ORs
                    (selectKeys, selectValues,selectOperators) = dataForSelectionValue
                    condition += ' ('
                    for i in range(len(selectKeys)):
                        condition += ' ' + selectKeys[i] + ' ' + selectOperators[i] + ' %s ' + 'OR'
                        values.append(selectValues[i])
                    condition = condition[:-2]
                    condition += ') '
                else:
                    # In such case we assume the condition is only in the key
                    condition += ' ' + dataForSelectionValue + ' ' + 'AND'
            elif type(dataForSelectionValue) == str and table.count(dataForSelectionValue.split('.')[0]):
                # In such case it is not a value, we are comparing to other table value!
                condition += ' ' + key + ' = ' + dataForSelectionValue + ' ' + 'AND'
            elif type(dataForSelectionValue) == tuple:    
                (selectValue,selectOperator) = dataForSelectionValue
                condition += ' ' + key + ' ' + selectOperator + ' %s ' + 'AND'
                values.append(selectValue)
            else:
                condition += ' ' + key + ' = %s ' + 'AND'
                values.append(dataForSelectionValue)
        condition = condition[:-3]
        q = 'SELECT ' + selectStatement + ' FROM ' + table + ' WHERE ' + condition + orderByStatement + limitOffsetStatement
        if toPrint:
            print cursor.mogrify(q,values)
        cursor.execute(q, values)
    else:
        if toPrint:
            print 'SELECT ' + selectStatement + ' FROM ' + table + orderByStatement + limitOffsetStatement
        cursor.execute('SELECT ' + selectStatement + ' FROM ' + table + orderByStatement + limitOffsetStatement)
    
    if not returnCursor:
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()
        return rows
    else:
        return cursor
 
def updateUniqueRow(connection, table, dataForSelection, dataForUpdating = None, columnNames = None, insertIfMissing = True, updateOnlyIfRowMissing = False, updateOnlyIfColumnMissing = False, toPrint = False):
    """ Updates a unique row in table. 
            - connection: valid connection instance 
            - table: LEDDB table name
            - dataForSelection: dictionary that contains the key columns values. There are the columns which values will be used to select the row, they have to be related to a UNIQUE constraint in the LEDDB definition
            - dataForUpdating: dictionary with the values that we want to update of the rest of the columns. Obviously the dictionary is columnname:value (same in dataForSelection)
            - columnNames: It is a list with column names. Indicate if we want to get the values of some columns of the updated row. It can be None in which case nothing is returned  
            - insertIfMissing: If the row is not there, we insert it (by default it is True)
            - updateOnlyIfRowMissing: The dataForUpdating is only used in the row was missing
            - updateOnlyIfColumnMissing: Each columnname:value in dataForUpdating is only used if the previous column value was null (None)
            - toPrint will indicate if the query will be printed to stdout"""    
    
    try:
        if columnNames != None:
            get = True
        else:
            get = False
        
        if len(dataForSelection) == 0:
            return None
        
        # If some of the parameters is None we return None
        for value in dataForSelection.values():
            if value == None:
                return None
            
        dataForUpdating = removeNoneValues(dataForUpdating)
        
        # We execute the select
        rows = select(connection,table, dataForSelection, columnNames, toPrint = toPrint)
        if len(rows) == 0:
            if insertIfMissing:
                allDataToAdd = dataForSelection.copy()
                allDataToAdd.update(dataForUpdating)
                insert(connection,table, allDataToAdd)
                if get:
                    return select(connection,table, dataForSelection, columnNames)[0]
                else:
                    return None
            else:
                return None
        elif len(rows) == 1 and len(dataForUpdating) and not updateOnlyIfRowMissing:
            updateKeys = dataForUpdating.keys()
            row = select(connection,table, dataForSelection, updateKeys)[0]
            for i in range(len(updateKeys)):
                if (not updateOnlyIfColumnMissing) or row[i] == None:
                    key = updateKeys[i]
                    modifiedDataForSelection = dataForSelection.copy()
                    if row[i] == None:
                        modifiedDataForSelection[key] = None
                    update(connection, table, {key:dataForUpdating[key]}, modifiedDataForSelection)
                    
            if get:
                return updateUniqueRow(connection, table, dataForSelection, None, columnNames, False)
            else:
                return None
        elif len(rows) != 1:
            raise Exception('error in ' + table + ' table: ' + str(len(rows)) + ' ' + str(dataForSelection.values()) + ' rows')
    
        if get:
            return rows[0]
        else:
            return None
    except Exception,e:
        # If some exception was launched it means that there is unmatching 
        # between the provided data and the one in the LEDDB, in such case we
        # assign a None
        print 'Error while getting unique row'
        print str(e)
        return None

def update(connection, table, dataToUpdate, dataForSelection):
    """  Update row/s in the table.
            - connection: valid connection instance
            - table: LEDDB table name
            - dataToUpdate: dictionary (columnname:value) with the values of the columns of the row/s to update
            - dataForSelection: dictionary (columnname:value) with the column values of the row/s to select"""
    setStatement = ''
    for key in dataToUpdate.keys():
        setStatement += ' ' + key + '=%s,'
    setStatement = setStatement[:-1]
    
    whereStatement = ''
    initialKeys = dataForSelection.keys()
    for key in initialKeys:
        if dataForSelection[key] == None:
            dataForSelection.pop(key)
            whereStatement += ' ' + key + ' IS NULL ' + 'AND'
        else:
            whereStatement += ' ' + key + '=%s ' + 'AND'
    whereStatement = whereStatement[:-3]
    
    allValues = dataToUpdate.values()
    allValues.extend(dataForSelection.values())
    
    cursor = connection.cursor()
    cursor.execute('UPDATE ' + table + ' SET ' + setStatement + ' WHERE ' + whereStatement, allValues)
    connection.commit()
    cursor.close()
    
def delete(connection, table, dataForSelection):
    """ Delete rows in the table.
            - connection: valid connection instance
            - table: LEDDB table name
            - dataForSelection: dictionary (columnname:value) with the column values of the rows to be deleted"""
    whereStatement = ''
    initialKeys = dataForSelection.keys()
    for key in initialKeys:
        if dataForSelection[key] == None:
            dataForSelection.pop(key)
            whereStatement += ' ' + key + ' IS NULL ' + 'AND'
        else:
            whereStatement += ' ' + key + '=%s ' + 'AND'
    whereStatement = whereStatement[:-3]
   
    cursor = connection.cursor()
    cursor.execute('DELETE FROM ' + table + ' WHERE ' + whereStatement, dataForSelection.values())
    connection.commit()
    cursor.close()

def removeNoneValues(data):
    """Remove from the dictionary the None values"""
    if data != None and len(data):
        initialKeys = data.keys()
        for key in initialKeys:
            if data[key] == None:
                data.pop(key)
        return data
    return {}


def getColValue(row, index = 0):
    """ Get the column value (indicated by index) of a row. It return None if the row is None """
    if row != None and len(row) > index:
        return row[index]
    else:
        return None

def getColValues(rows, index = 0):
    """ Get the column values (indicated by index) for some rows. If some row is None, the value for that row is None """
    values = []
    for row in rows:
        values.append(getColValue(row, index))
    return values

def getTableColValues(connection, table, index = 0):
    """ Get the column values (indicated by index) for all the rows of a table. If some row is None, the value for that row is None """
    return getColValues(select(connection, table), index)

def getTableNumRows(connection, table):
    """ Get the number of rows of a table"""
    return int(select(connection, table, columnNames = ['count(*)', ])[0][0]) 

def copyProp(connection, ldsbpIdFill, ldsbpIdRead, onlymss = False, onlyldsbp = False, columnNames = [INTTIME, NUMCHAN, DESCR, FLAGGED, AVERAGED, CALIBRATED, DIRDEPCAL, DIRINDEPCAL]):
    """"Get the column values (from the columns indicated by column names) from a LDSBP indicated by ldsbpIdRead and copy them in ldsbpIdFill. 
    It also fills the BW and centFreqs in the MSs related to ldsbpIdFill.
    If onlymss is specified it does not copy the LDSBP properties, only the BW and centFreqs in the MSs
    If onlyldsbp is specified it only copies the LDSBP properties, i.e. not the BW and centFreqs in the MSs"""
    if onlymss and onlyldsbp:
        raise Exception('ERROR: only one of (onlymss, onlyldsbp) can be True')
    readldsbprows = select(connection, LDSBP, {LDSBP+ID:ldsbpIdRead,}, columnNames)
    if len(readldsbprows):
        if not onlymss:
            dataForLDSBPUpdating = {}
            for i in range(len(columnNames)):
                dataForLDSBPUpdating[columnNames[i]] = readldsbprows[0][i]
            updateUniqueRow(connection, LDSBP, {LDSBP+ID:ldsbpIdFill}, dataForLDSBPUpdating)
        if not onlyldsbp:
            msrows = select(connection, MS, {LDSBP+ID:ldsbpIdFill,}, columnNames = [MS+ID,SBINDEX])
            for msrow in msrows:
                msrowsread = select(connection, MS, {LDSBP+ID:ldsbpIdRead,SBINDEX:msrow[1]}, columnNames = [CENTFREQ, BW])
                if len(msrowsread):
                    updateUniqueRow(connection, MS, {MS+ID:msrow[0]}, {CENTFREQ:msrowsread[0][0],BW:msrowsread[0][1]})

def copyPropEoRToTarget(connection, storeFill, storeRead):
    """ Using previous method, this method select all the LDSBP from certain store and copies their properties to their equivalent in another store
    This is used for example to copy from the EoR properties to Target properties 
    This us useful because in Target we can not compute the properties, the updater process run in EoR cluster and we not have access to the files"""
    storeFillLDSBPRows = select(connection, LDSBP, {STORE: storeFill}, columnNames = [LDSBP+ID,LDSB+ID,VERSION])
    for (ldsbpId,ldsbId,version) in storeFillLDSBPRows:
        storeReadLDSBPRows = select(connection, LDSBP, {LDSB+ID:ldsbId, VERSION: version, STORE: storeRead, INTTIME: (0,'>')}, columnNames = [LDSBP+ID,])
        if len(storeReadLDSBPRows): 
            copyProp(connection, ldsbpId, storeReadLDSBPRows[0][0])
    connection.commit()
    connection.close()