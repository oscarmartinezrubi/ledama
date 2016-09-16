################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama.PrettyTable import PrettyTable

class GetBaselines(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('station1','s','Station 1', mandatory=False, helpmessage=' regular expression pattern, use POSIX regex. For example, providing "RS" will select the baselines in which station1 is a Remote Station. Remember that Station2 >= Station1 (in string comparison)')
        options.add('station2','t','Station 2', mandatory=False, helpmessage=' regular expression pattern')
        options.add('operator','o','Operator', helpmessage=' to use between patterns', default='AND')
        options.add('full','f','Full row',helpmessage='is shown', default=False)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        # the information
        information = 'Get the Baseline Ids using POSIX regular expression matching'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   


    def process(self,station1,station2,operator,full,dbname,dbuser,dbhost):
        
        qDict = {}
        if full:
            cols = ','.join([BASELINE+ID,STATION1,STATION2])
        else:
            cols = BASELINE+ID
            
        tab = 'SELECT ' + cols + ' FROM ' + BASELINE
        
        if station1 != "" or station2 != "":
            tab += ' WHERE ' 
            if station1 != "":
                tab += STATION1 + ' ~ %(station1)s ' + operator + ' ' 
                qDict['station1'] = station1
            if station2 != "":
                tab += STATION2 + ' ~ %(station2)s ' + operator + ' ' 
                qDict['station2'] = station2
            tab = tab[:-(len(operator + ' '))]
                
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        cursor = connection.cursor()
        cursor.execute(tab, qDict)
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        baselineIds = []
        if full:
            print cursor.query
            pTable = PrettyTable(['baselineId','station1','station2'])
            for row in rows:
                pTable.add_row(row)
                baselineIds.append(str(row[0]))
            print pTable.get_string()
            print 'There are ' + str(len(baselineIds)) + ' BaselineIds matching the selection: ' 
        else:
            for row in rows:
                baselineIds.append(str(row[0]))
        print ','.join(baselineIds)