################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

MSKEYWORD = '--MS--'
FMSKEYWORD = '--FMS--'
PMSKEYWORD = '--PMS--'

class ExecuteCommandOnData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('command', 'c', 'Command', helpmessage = '. There are 3 special keywords: ' + MSKEYWORD + ', will be replaced by the MS name. ' + FMSKEYWORD + ' will be replaced by the MS full path. ' + PMSKEYWORD + ' will be replaced by the full parent path of the MS.')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('usenfs', 'u', 'Use NFS', helpmessage = ' instead of ssh', default = False)
        options.add('logspath', 'l', 'Logs path', mandatory = False)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        
        # the information
        information = """Execute a command in all the MSPs pointed by the refFile. Before execution it is recommended to use query option."""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    # Function used for the tasksdistributor to run a command
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (absPath,command) = what 
        if self.query:
            return (absPath, command)
        else:
            if self.logsPath != None:
                td.execute(command, True) #we redirect output to log
            else:
                (out,err) = td.execute(command)
                if err != '':
                    return (absPath, err[:-1])
                elif out != '':
                    return (absPath, out[:-1])
    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what[0]) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
    
    def process(self, reffile, command, numprocessors, numnodes, usenfs, logspath, query):
        self.query = query
        referenceFile = ReferenceFile(reffile)
        self.logsPath = None
        if (not self.query) and logspath != '':
            self.logsPath = os.path.abspath(logspath)
        
        if command.count('"') and command.count("'") and not usenfs:
            print 'ERROR: When using SSH the command cannot contain both " and ' + "'"
            return
        elif command.count('"'):
            separator = "'"     
        elif command.count("'"):
            separator = '"'
        else:
            separator = "'"
        
        whats = []
        for i in range(len(referenceFile.absPaths)):
            absPath = referenceFile.absPaths[i]
            mspName = msoperations.getMeasurementSetName(absPath)
            parentPath = msoperations.getParentPath(absPath)
            if not usenfs:
                whats.append((absPath, 'ssh ' + referenceFile.nodes[i] + ' ' + separator + command.replace(MSKEYWORD,mspName).replace(FMSKEYWORD,absPath).replace(PMSKEYWORD, parentPath) + separator))    
            else:
                # USE NFS
                whats.append((absPath, command.replace(MSKEYWORD,mspName).replace(FMSKEYWORD,'/net/' + referenceFile.nodes[i] + absPath).replace(PMSKEYWORD, '/net/' + referenceFile.nodes[i] + parentPath)))
        
        # Run it
        retValuesOk = td.distribute(referenceFile.nodes, whats, self.function, numprocessors, numnodes, logFolder = self.logsPath, getLogFileName = self.getLogFileName)[0]
        td.showOk(retValuesOk)    