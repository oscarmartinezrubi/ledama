#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
class QueryTable:
    """Query Table. It makes an object from a table in QueryTables"""
    def __init__(self,description=''):
        """ Initialize all the variables with empty lists"""
        self.description = description
        self.names = [] # name of the column as on the DB table
        self.tables = [] # DB table name of which column belongs to
        self.headers = [] # what will be shown in the LEDDB web header
        self.fullnames = [] # full column path with the DB
        self.formatfullnames = [] # full column path within the DB plus some additional method
        self.informations = [] # what will be shown in the LEDDB web header help message
        self.showns = [] # indicates if a column is shown by default in the LEDDB web
        self.filterables = [] # indicates if the row can be used to filter results in current LEDDB web table
        self.inforvalues = [] # indicates if a column is used when making the special query which uses the values (only for Diagnostic tables)
        
        self.defaultorder = None
        
    def add(self, name, table, header, information, shown = True, filterable = True, inForValue = False, formatMethod = None):
        """ Add a column """
        if name in self.names:
            raise Exception('Duplicated ' + name)
        if header in self.headers:
            raise Exception('Duplicated ' + header)
        self.names.append(name)
        self.tables.append(table)
        fullname = table +'.'+name
        self.headers.append(header)
        self.fullnames.append(fullname)
        if formatMethod != None:
            self.formatfullnames.append(formatMethod(fullname))
        else:
            self.formatfullnames.append(fullname)
        self.informations.append(information)
        self.showns.append(shown)
        self.filterables.append(filterable)
        self.inforvalues.append(inForValue)
        
    def getDescription(self):
        return self.description
    
    def getNames(self, onlyshown=False, onlyfilterable=False, onlyinforvalues=False):
        """ Gets all the names, if onlyshown is True it returns the only the names of the columns which have shown=True.
        The same for the onlyfilterable and onlyinforvalues """
        # check that max 1 only____ is selected
        c = 0
        for b in (onlyshown, onlyfilterable, onlyinforvalues):
            if b:
                c += 1
        if c > 1:
            raise Exception('ERROR: Only one (onlyshown, onlyfilterable, onlyinforvalues) can be selected')
        names = []
        if onlyshown:
            for i in range(len(self.names)):
                if self.showns[i]:
                    names.append(self.names[i])
        elif onlyfilterable:
            for i in range(len(self.names)):
                if self.filterables[i]:
                    names.append(self.names[i])
        elif onlyinforvalues:
            for i in range(len(self.names)):
                if self.inforvalues[i]:
                    names.append(self.names[i])
        else:
            names = list(self.names) 
        return names
    
    def getAttributes(self, allattributes, names):
        """Get all the attributes if no names is specified or the related atts if names is given"""
        if names == None:
            return list(allattributes)
        else:
            attributes = []
            for name in names:
                attributes.append(allattributes[self.names.index(name)])
            return attributes
    
    def getTables(self, names = None):
        """ Get the tables related to all the columns
        If names != None we only return the tables related to the specified names"""
        return self.getAttributes(self.tables, names)
    
    def getHeaders(self, names = None):
        """ Get the headers of all the columns
        If names != None we only return the headers related to the specified names"""
        return self.getAttributes(self.headers, names)
    
    def getFullNames(self, names = None):
        """ Get the full names of all the columns, i.e. table.name.
        If names != None we only return the fullnames related to the specified names"""
        return self.getAttributes(self.fullnames, names)
    
    def getFormatFullNames(self, names = None):
        """ Get the formatted full names of all the columns, i.e. table.name formatted (for example tochar(table.name).
        If names != None we only return the formatfullnames related to the specified names"""
        return self.getAttributes(self.formatfullnames, names)
    
    def getInformations(self, names = None):
        """ Get the header informations of all the columns
        If names != None we only return the informations related to the specified names"""
        return self.getAttributes(self.informations, names)
    
    def getShowns(self, names = None):
        """ get the showns"""
        """ Get, for all the columns, the boolean indicating if a column should be shown by default
        If names != None we only return the informations related to the specified names"""
        return self.getAttributes(self.showns, names)

    def getFilterables(self, names = None):
        """ get the filterables"""
        """ Get, for all the columns, the boolean indicating if a column can be used to filter queries to the table
        If names != None we only return the informations related to the specified names"""
        return self.getAttributes(self.filterables, names)
        
    def getInForValues(self, names = None):
        """ get the inforvalues"""
        """ Get, for all the columns, the boolean indicating if a column should be used when querying the values 
        If names != None we only return the informations related to the specified names"""
        return self.getAttributes(self.inforvalues, names)
    
    def headersToNames(self, headers):
        """ Get the names from headers (both name and header are unique within a queryTable)"""
        names = []
        for header in headers:
            names.append(self.names[self.headers.index(header)])
        return names
    
    def setDefaultOrder(self, names):
        """ Set the default order """
        for name in names:
            if name not in self.names:
                raise Exception(' ' + name + ' in not a possible column name')
        self.defaultorder = names
        
    def getDefaultOrder(self,):
        """ Get the default order """
        return self.defaultorder