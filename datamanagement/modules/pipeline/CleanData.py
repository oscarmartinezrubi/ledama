################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

def functionForSSH(absPath, node, column):
    size = msoperations.cleanBand(absPath, column)
    if type(size) == str:
        # Error happened
        error = size
        print error
        size = -1
    else:
        print absPath + ' ' + node + ' ' + str(size)
    
class CleanData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile',helpmessage=', it will be updated with the new sizes')
        options.add('column', 'c', 'Column',helpmessage=' to be cleaned', default='CORRECTED_DATA')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('logspath', 'l', 'Logs path', mandatory = False)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Clean the indicated column in the MSPs pointed by RefFile'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    # Function used for the tasksdistributor
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        # Define commands
        commandAdd = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + functionForSSH.__name__ + '(\\\"' + absPath +'\\\", \\\"' + node +'\\\", \\\"' + self.column +'\\\")"'
        if node != currentNode:
            commandAdd =  'ssh ' + node + " '" + commandAdd + "'"
        if self.query:
            return (absPath, commandAdd)
        else:
            (addout, adderr) = td.execute(commandAdd)     
            if adderr != '':
                raise Exception(adderr)
            try:
                for line in addout.split('\n'):
                    if line.startswith(absPath):
                        (absPath,node,size) = line.split(' ')
                        size = int(size)
                        return (absPath,node,size)
                raise Exception(addout)
            except:
                raise Exception(addout)

    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
    
    def process(self, reffile, column, numprocessors, numnodes, logspath,query,initfile):
        self.column = column
        self.query = query
        self.initfile = os.path.abspath(initfile)
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = utils.formatPath(os.path.abspath(logspath))
        
        referenceFile = ReferenceFile(reffile)
        numSBs = len(referenceFile.absPaths)
        newSizesDict = {}
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, 
            self.function, numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        for retValueOk in sorted(retValuesOk):
            if retValueOk != None:
                if len(retValueOk) == 3:
                    (absPath,node,size) = retValueOk
                    newSizesDict[(absPath, node)] = size
                else:
                    print retValueOk[1]
        if not self.query:        
            if len(newSizesDict) == numSBs:
                for i in range(numSBs):
                    newSize = newSizesDict[(referenceFile.absPaths[i], referenceFile.nodes[i])]
                    if newSize > 0:
                        referenceFile.sizes[i] = newSize
                    else:
                        referenceFile.refFreqs[i] = 'ERROR'
                
                # Delete the old file with the old sizes
                os.system('rm ' + reffile)
                # Write the file with the updated data
                referenceFile.write()
                print reffile + ' has been updated with new sizes.'
            else:
                print 'ERROR while updating the ' + reffile + '. ' + str(len(newSizesDict)) + ' SBs were detected of ' + str(numSBs)
                td.showKoFirst(retValuesKo) 