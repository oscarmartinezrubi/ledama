################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama import utils
from ledama.leddb.Naming import *
from ledama.PrettyTable import PrettyTable

class GetStations(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('anttype','a','Antenna Type', mandatory=False)
        options.add('loctype','l','LocationType', mandatory=False)
        options.add('full','f','Full row',helpmessage=', it will print all the information for each row', default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)  
        
        # the information
        information = 'Get the Stations'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   


    def process(self,anttype,loctype,full,dbname,dbuser,dbhost):
        
        qDict = {}
        if full:
            cols = ','.join([NAME, ANTTYPE, LOCATIONTYPE])
        else:
            cols = NAME
            
        tab = 'SELECT ' + cols + ' FROM ' + STATION
        
        if anttype != "" or loctype != "":
            operator = 'AND'
            tab += ' WHERE ' 
            if anttype != "":
                tab += ANTTYPE + ' = %(antennatype)s ' + operator + ' ' 
                qDict['antennatype'] = anttype
            if loctype != "":
                tab += LOCATIONTYPE + ' = %(locationtype)s ' + operator + ' ' 
                qDict['locationtype'] = loctype
            tab = tab[:-(len(operator + ' '))]
                
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        cursor.execute(tab, qDict)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        stations = []
        if full:
            print cursor.query
            pTable = PrettyTable(['Station','Antenna','Location'])
            for row in rows:
                pTable.add_row(row)
                stations.append(row[0])
            print pTable.get_string()
            print 'There are ' + str(len(stations)) + ' stations matching the selection: ' 
        else:
            for row in rows:
                stations.append(row[0])
        print ','.join(stations)