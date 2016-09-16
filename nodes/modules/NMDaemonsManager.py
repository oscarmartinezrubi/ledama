################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
import ledama.tasksdistributor as td
import ledama.config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
import ledama.leddb.LEDDBOps as LEDDBOps

COMMANDFORRESTARTDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/nodes/NodeMonitorDaemon.py restart'
COMMANDFORSTARTDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/nodes/NodeMonitorDaemon.py start'
COMMANDFORSTOPDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/nodes/NodeMonitorDaemon.py stop'

INIT = 'INITIALIZED'
STOP = 'STOPPED'
STATUS_ERROR_MULTIPLE = 'ERROR - MULTIPLE RUNNING'
STATUS_ERROR_NOT_RUNNING = 'ERROR - NOT RUNNING'
STATUS_OK = 'RUNNING'

class NMDaemonsManager(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('check','c','Check the daemons',default=False)
        options.add('init','i','Initialize the daemons',default=False)
        options.add('stop','s','Stop the daemons',default=False)
        options.add('createnmdb','b','Create NMDB table',helpmessage=' (remember to set in the config file the proper NMDB name and host). This step must be done after createdb. Remember also to check that the users that will access the DB are added (with proper permissions)',default=False)
        options.add('deletenmdb','d','Delete the NMDB table',default=False)
        options.add('nodes','n','Nodes to use',default='node001-080/lofareor01')

        # the information
        information = 'Manager for the NodeMonitorDaemons'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function to stop all the daemons
    def functionStopDaemon(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier 
        (out,err) = td.execute("ssh " + node + " '" + COMMANDFORSTOPDEAMONS + "'")
        if err != '':
            raise Exception(err[:-1])
        elif out != '':
            raise Exception(out[:-1])
        else:
            return (node, STOP)
    
    # Function to initialize the daemons
    def functionInitializeDaemon(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier 
        (out,err) = td.execute("ssh " + node + " '" + COMMANDFORSTARTDEAMONS + "'")
        if err != '':
            raise Exception(err[:-1])
        elif out != '':
            raise Exception(out[:-1])
        else:
            return (node, INIT)
        
    def functionCheckDaemon(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier 
        (out,err) = td.execute("ssh " + node + " 'ps aux | grep NodeMonitor'")
        if err != '':
            raise Exception(err[:-1])
        lines = out.split('\n')
        counter = 0
        for line in lines:
            if line != '' and (line.count(COMMANDFORSTARTDEAMONS) or line.count(COMMANDFORRESTARTDEAMONS)):
                counter += 1
        message = None
        if counter == 0:
            status = STATUS_ERROR_NOT_RUNNING
        elif counter > 1:
            status = STATUS_ERROR_MULTIPLE
            message = counter
        else:
            status = STATUS_OK
        return (node,status,message)
    
    def process(self, check, init, stop,createnmdb, deletenmdb, nodes):
        if not (check or init or stop or createnmdb or deletenmdb):
            print 'Select what you want to do! (use -h)'
            return
        
        if (init or stop or createnmdb or deletenmdb) and self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        nodestouse = utils.getNodes(nodes)
        
        if stop or init or check:
            function = None
            if stop:
                print 'Stopping NM daemons...'
                function = self.functionStopDaemon
            elif init:
                print 'Initializing NM daemons...'
                function = self.functionInitializeDaemon
            else:
                print 'Checking NM daemons...'
                function = self.functionCheckDaemon
                
            (retValuesOk,retValuesKo) = td.distribute(nodestouse, nodestouse, function, 1, len(nodestouse))    
            if len(retValuesKo):
                print 'ERROR (' + str(len(retValuesKo)) + '): '
                for retValueKo in retValuesKo:
                    print retValueKo
            
            if stop or init:
                nodes = []
                for (node,message) in sorted(retValuesOk):
                    nodes.append(node)
                if len(retValuesOk):
                    print message + ' (' + str(len(nodes)) + '): ' + ','.join(nodes)
            else:
                dictStatus = {STATUS_OK: [], STATUS_ERROR_NOT_RUNNING : [], STATUS_ERROR_MULTIPLE : []}
                for (node,status,message) in sorted(retValuesOk):
                    if message == None:
                        dictStatus[status].append(node)
                    else:
                        dictStatus[status].append(node + '(' + str(message) + ')')
                for status in dictStatus:
                    if len(dictStatus[status]):
                        print status + ' (' + str(len(dictStatus[status])) + '): ' + ','.join(dictStatus[status])
        elif createnmdb or deletenmdb:
            
            connection = Connector(lconfig.NODE_MONITOR_DB_NAME, self.userName, lconfig.NODE_MONITOR_DB_HOST).getConnection()
            if connection == None:
                print 'Error getting the connection'
                return
            # Get the cursor
            cursor = connection.cursor()
            if createnmdb:
                
                print 'Create Host table in NMDB'
                # Create Host Table
                cursor.execute("""CREATE TABLE """ + HOST + """ (
                    """ + NAME + """ VARCHAR(20) PRIMARY KEY,
                    """ + REQUIRESUPDATE + """ BOOLEAN DEFAULT FALSE,
                    """ + USAGEMON + """ TEXT,
                    """ + GPUUSAGEMON + """ TEXT,
                    """ + STORAGEMON + """ TEXT,
                    """ + NETMON + """ TEXT
                )""")
                cursor.execute("GRANT SELECT ON " + HOST + " TO public")
                
                for node in nodestouse:
                    print 'Adding ' + node
                    LEDDBOps.updateUniqueRow(connection, HOST, {NAME:node,})
            else: #deletedb
                cursor.execute("""DROP TABLE """ + HOST + """ CASCADE""")
