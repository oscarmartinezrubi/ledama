################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
import ledama.tasksdistributor as td
import ledama.config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

COMMANDFORRESTARTDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/datamanagement/FDTDaemon.py restart'
COMMANDFORSTARTDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/datamanagement/FDTDaemon.py start'
COMMANDFORSTOPDEAMONS = 'python ' + lconfig.LEDAMA_ABS_PATH + '/datamanagement/FDTDaemon.py stop'

INIT = 'INITIALIZED'
STOP = 'STOPPED'
STATUS_ERROR_MULTIPLE = 'ERROR - MULTIPLE RUNNING'
STATUS_ERROR_NOT_RUNNING = 'ERROR - NOT RUNNING'
STATUS_OK = 'RUNNING'

class FDTDaemonsManager(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('check','c','Check the daemons',default=False)
        options.add('init','i','Initialize the daemons',default=False)
        options.add('stop','s','Stop the daemons',default=False)
        options.add('nodes','n','Nodes to use',default='node011-074')
        
        # the information
        information = 'Manager for the FDTDaemons.'
        
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
        (out,err) = td.execute("ssh " + node + " 'ps aux | grep FDT'")
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
    
    def process(self, check, init, stop, nodes):
        if not (check or init or stop):
            print 'Select what you want to do! (use -h)'
            return
        if (init or stop) and self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        nodestouse = utils.getNodes(nodes)
        function = None
        if stop:
            print 'Stopping FDT daemons...'
            function = self.functionStopDaemon
        elif init:
            print 'Initializing FDT daemons...'
            function = self.functionInitializeDaemon
        elif check:
            print 'Checking FDT daemons...'
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
            
            
                