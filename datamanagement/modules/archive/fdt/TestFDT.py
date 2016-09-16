################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
import ledama.config as lconfig
from ledama import utils
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class TestFDT(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('nodes', 'u', 'Nodes')
        options.add('statuspath', 's', 'Status path', helpmessage = ' where the logs are stored')
        options.add('remotehost', 'r', 'Remote host', default='lotar1.staging.lofar')
        options.add('remoteport', 't', 'Port', default = 20001)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 10)
        
        # the information
        information = 'Test the FDT connection'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        command = 'ssh ' + node + " 'java -jar " + lconfig.FDT_PATH + " -c " + self.remotehost + " -p " + str(self.remoteport) + " -nettest'"
        td.execute(command, True)

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return what + '.log'

    def process(self, nodes, statuspath, remotehost, remoteport, numprocessors, numnodes):
        self.statuspath = os.path.abspath(statuspath)
        self.remotehost = remotehost
        self.remoteport = remoteport
        if os.path.isdir(self.statuspath):
            os.system('rm -r ' + self.statuspath)

        identifiers = []
        whats = []
        for node in utils.getNodes(nodes):
            for i in range(numprocessors):
                identifiers.append(node)
                whats.append(node + '_' + ('%03d' % i))
        
        # Run it
        retValuesKo = td.distribute(identifiers, whats, self.function, 
            numprocessors, numnodes, logFolder = self.statuspath, getLogFileName = self.getLogFileName)[1]
        for retValueKo in retValuesKo:
            print retValueKo