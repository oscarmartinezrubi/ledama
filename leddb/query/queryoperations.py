#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import *

# This python module is used to define operations to query the LEDDB using the QueryTables

# Get conditions for a JOIN or a WHERE clauses
def getConditions(table, queryConditions):
    tableConditions = []
    if table == LDSHASSTATION:
        condnames = queryConditions.getNames()
        if (STATION in condnames) and (NUMSELSTATIONS in condnames):
            tableConditions.append(LDSHASSTATION + '.' + STATION + " " + queryConditions.getOperation((LDSHASSTATION,STATION)) + " %(" + LDSHASSTATION + '.' + STATION + ")s")
    else:
        condkeys = queryConditions.getKeys()
        for condkey in condkeys:
            (condtable,condname) = condkey
            if condtable == table:
                operation = queryConditions.getOperation(condkey)
                if operation.lower().count('null') != 0:
                    # There is a special case where operation can be "is null" or "is not null",
                    # in this case we do not need to add a value
                    tableConditions.append(table + '.' + condname + " " + operation)
                else:
                    tableConditions.append(table + '.' + condname + " " + operation + " %(" + condtable + '.' + condname + ")s")
                # remove the query condition
                queryConditions.remove(condkey)
    return tableConditions

def initFromStatement():
    return ''

def updateFromStatement(fromStatement, currTab, nextTab, currTabConditions, nextTabConditions = None, oTabs = None):
    if currTab == LDSHASSTATION:
        if len(currTabConditions):
            sel = "SELECT " + LDS + " AS " + NAME + "," + STATION + " FROM " + LDSHASSTATION + " WHERE ("   + currTabConditions.pop() + ")"    
            fromStatement += "(SELECT s." + NAME + ",count(s." + STATION + ") FROM (" + sel +  ") as s GROUP BY s." + NAME + " ) as r JOIN " + LDSJOINED + " ON (r." + NAME + " = "+LDSJOINED+"." + NAME + " AND r.count=%(" + LDSHASSTATION + '.' + NUMSELSTATIONS + ")s" 
            if nextTabConditions != None:
                for i in range(len(nextTabConditions)):
                    fromStatement += ' AND ' + nextTabConditions.pop()
            fromStatement += ")"
        return fromStatement
    
    elif currTab == LDSJOINED:
        matchCol1 = LDS
        matchCol2 = NAME
    else:
        if currTab == LDSBJOINED:
            matchCol1 = LDSB+ID
        elif currTab == LDSBPJOINED:
            matchCol1 = LDSBP+ID
        elif currTab == GAINMOVIEHASMS:
            matchCol1 = GAINMOVIE+ID
        elif (currTab in (QBSTAT, QTSTAT, QFSTAT)) and (nextTab in (QKIND, BASELINE)):
            matchCol1 = nextTab+ID
        else:
            matchCol1 = MS+ID
        matchCol2 = matchCol1  
    # If there are any conditions to apply to current table or current table is used in the orderBr or queried columns or no tab has been defined so far
    if len(currTabConditions) or fromStatement != "" or ((oTabs == None) or (currTab in oTabs)):
        if fromStatement == "":
            fromStatement = currTab
        fromStatement += ' JOIN ' + nextTab + ' ON (' + nextTab + '.' + matchCol1 + '=' + currTab + '.' + matchCol2
        for i in range(len(currTabConditions)):
            fromStatement += ' AND ' + currTabConditions.pop()
        # We add the conditions of the next table too (if any)
        if nextTabConditions != None:
            for i in range(len(nextTabConditions)):
                fromStatement += ' AND ' + nextTabConditions.pop()
        fromStatement += ')'
    return fromStatement    

def finishFromStatement(fromStatement, currTab, currTabConditions):
    # MSP is the base table of this query, we set tab to MSP if it is not defined yet
    if fromStatement == "":
        fromStatement = currTab
   
    # If there is still some MSP condition to be added, this is the moment
    if len(currTabConditions):
        fromStatement += ' WHERE '
        for i in range(len(currTabConditions)):
            fromStatement += currTabConditions.pop() + ' AND '
        fromStatement = fromStatement[:-4]
    return fromStatement    

def getSelectQuery(queryTable, cols, fromStatement, orderBy, offset, limit, desc = False, distinct = False, formatcols = False):
    sel = 'SELECT '
    if distinct:
        sel += 'DISTINCT '
    
    if formatcols:
        ucols = queryTable.getFormatFullNames(names=cols)
    else:
        ucols = queryTable.getFullNames(names=cols)
    
    sel += ','.join(ucols) + ' FROM ' + fromStatement
    if orderBy != None:
        sel += ' ORDER BY ' + ','.join(queryTable.getFullNames(names=orderBy))
        if desc:
            sel += ' DESC '
    if offset != None:
        sel += ' OFFSET ' + str(offset)
    if limit != None:
        sel += ' LIMIT ' + str(limit)
    return sel
