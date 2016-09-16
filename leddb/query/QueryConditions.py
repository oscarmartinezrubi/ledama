#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
class QueryConditions:
    """Query Conditions"""
    def __init__(self,data={}):
        """ Initialize the query conditions dictionary, 
        this query conditions are related to the real DB tables, 
        not to the QueryTables (the ones used in the LEDDB web and the getters)"""
        self.data = data
        
    def add(self, queryTable, name, value, operation = 'IN'):
        """ Add a query condition given a queryTable instance, the name of the column from which we want to add the condition
        , the operation and the value."""
        # First we get the table name related to this colname (and the queryTable)
        # We use strip because it may be that the name has some added white-spaces 
        # (this will happen when having multiple conditions on the same column)
        table = queryTable.getTables(names = [name.strip(),])[0] 
        filterable = queryTable.getFilterables(names = [name.strip(),])[0]
        if not filterable:
            raise Exception('ERROR: You tried to add a condition on a non-filterable column in (' + name + ').')
        if (table, name) in self.data:
            raise Exception('ERROR: There is already a condition in (' + table + ',' + name + '). Suggestion: user white-space after the name, i.e. "' + name + ' "')
        if (type(value) in (unicode,str)) and (value.lower().count('null') > 0) and (operation not in (None, '')):
            if operation == '=':
                operation =  ' is ' + str(value)
            elif operation == '!=':
                operation =  ' is not ' + str(value)
            else:
                operation +=  str(value)
        self.data[(table, name)] = (operation, value)
        
    def remove(self, key):
        """ Remove a condition"""
        self.data.pop(key)
    
    def copy(self):
        """ Gets a new QueryConditions instance which content is the same as the current one"""
        copyQC = QueryConditions()
        copyQC.data = dict(self.data)
        return copyQC
    
    def update(self, qc):
        """ Updates the current queryCondition with the contents of another instance"""
        self.data.update(qc.data)
        
    def has(self, key):
        """ Check if there is already a condition on a column (within a table which is obtained thants to the queryTable) """
        return (key in self.data)
    
    def getKeys(self):
        """ Get all the conditions names"""
        return sorted(self.data.keys())
    def getNames(self):
        """Get the names part of the keys"""
        names = []
        for key in self.getKeys():
            names.append(key[1])
        return names
    def getTables(self):
        """Get the tables part of the keys (DB tables)"""
        tables = []
        for key in self.getKeys():
            tables.append(key[0])
        return tables
    
    def getNum(self):
        return len(self.data)
    def getOperation(self, key):
        return self.data[key][0]
    def getValue(self, key):
        return self.data[key][1]
    def getValuesDict(self):
        vDict = {}
        for (table,name) in self.getKeys():
            vDict[table + '.' + name] = self.getValue((table,name))
        return vDict
