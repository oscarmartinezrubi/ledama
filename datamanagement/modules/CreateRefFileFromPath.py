################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama import utils
from ledama import msoperations
from ledama import nodeoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

# EXTRA METHOD OUT OF THE CLASS, this necessary for ssh callings of such method without needing to create the instance 
def sshFunctionGetSizeFreq(absPath):
    size = msoperations.getSize(absPath)
    beamIndex = msoperations.getBeamIndex(absPath)
    try:
        freq = '%.03f' % msoperations.getCentralFrequency(msoperations.getTable(absPath)) 
    except:
        freq = '?'
    print absPath + ' ' + str(freq) + ' ' + str(size) + ' ' + str(beamIndex)

# EXTRA METHOD OUT OF THE CLASS, this necessary for ssh callings of such method without needing to create the instance 
def sshFunctionGetPaths(pathToCheck, packStr):
    pack = utils.booleanStringToBoolean(packStr)
    for mspPath in nodeoperations.getMSsFromPath(pathToCheck):
        mspPathLower = mspPath.lower()
        if (pack and (mspPathLower.endswith('.tar') or mspPathLower.endswith('.bvf'))) or (not pack and not mspPathLower.endswith('.tar') and not mspPathLower.endswith('.bvf')):
            print mspPath


class CreateRefFileFromPath(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        # Define the options
        options = LModuleOptions()
        options.add('inputpath', 'i', 'Path[s] (if multiple, comma-separated without blank spaces)')
        options.add('nodes', 's', 'Nodes', helpmessage = ' where we want to search the data. For example if we want to use the node001 and from the node010 to  node020 (both included) we should use node1,10-20', default = '001-080')
        options.add('output', 'o', 'Output RefFile', default = utils.getHome(userName) + "/myRefFile" + ReferenceFile.EXTENSION)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 8)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('pack', 't', 'Only TAR/BVF data is considered', default = False)
        options.add('onlypath', 'x', 'Only path/node are checked', helpmessage = ', frequency and size are ignored', default = False)
        options.add('initfile', 'f', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        
        # the information
        information = """Creates a RefFile by specifying paths and/or nodes (without using the LEDDB). 
You can decide if only packed data is considered (i.e. TAR or BVF files). 
In addition you can also specify to only check the locations (paths/nodes). 
In this way the measurement sets are not read (this can be specially useful if you think that the measurement set may be locked)"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function used for the tasksdistributor   
    def functionGetSizeFreq(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        commandSearch = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + sshFunctionGetSizeFreq.__name__ + '(\\\"' + absPath +'\\\")"'
        if node != currentNode:
            commandSearch =  "ssh " + node + " '" + commandSearch + "'"
        (searchout, searcherr) = td.execute(commandSearch)
        if searcherr != '':
            raise Exception(searcherr[:-1])
        try:
            (absPath, refFreq, size, beamIndex) = searchout.split('\n')[0].split(' ')
            size = int(size)
        except:
            refFreq = '?'
            size = '?'
            beamIndex = '?'
        return (node, absPath, refFreq, size, beamIndex)
    
    def functionGetPaths(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        commandSearch = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + sshFunctionGetPaths.__name__ + '(\\\"' + absPath +'\\\", \\\"' + str(self.pack) +'\\\")"'
        if node != currentNode:
            commandSearch =  "ssh " + node + " '" + commandSearch + "'"
        (searchout, searcherr) = td.execute(commandSearch)
        if searcherr != '':
            raise Exception(searcherr[:-1])
        absPaths = []
        for line in searchout.split('\n'):
            if line != '':
                absPaths.append(line.strip())
        return (node, absPaths)
    
    def process(self, inputpath, nodes, output, numprocessors, numnodes, pack, onlypath, initfile):
        self.initfile = os.path.abspath(initfile)
        self.onlypath = onlypath
        self.pack = pack
        # Check for already existing output files
        if os.path.isfile(output):
            print 'WARNING: ' + output + ' already exists. It will be overwritten...'
        
        nodesToUse = utils.getNodes(nodes)
        # We prepare a list of nodes-paths to be checked in each node
        cnodes = []
        cpaths = []
        for pathToCheck in inputpath.split(','):
            for node in nodesToUse:
                cnodes.append(node)
                cpaths.append(pathToCheck)
        
        # We get the several MS paths from the different nodes
        print 'Getting MS paths...'
        (retValuesOk, retValuesKo) = td.distribute(cnodes, cpaths, self.functionGetPaths, numprocessors, numnodes)
        td.showKoAll(retValuesKo)
        
        refFreqs=[]
        sizes=[]
        nodes=[]
        absPaths=[]
        beamIndexes=[]
        if onlypath:
            for retValueOk in sorted(retValuesOk):
                (node, nodeAbsPaths) = retValueOk
                for absPath in sorted(nodeAbsPaths):
                    absPaths.append(absPath)
                    nodes.append(node)
                    refFreqs.append('?')
                    sizes.append('?')
                    beamIndexes.append('?')
        else:
            # We will ssh again for the remaining information (freq and size)
            identifiers = []
            whats = []
            for retValueOk in sorted(retValuesOk):
                (node, nodeAbsPaths) = retValueOk
                for absPath in nodeAbsPaths:
                    whats.append(absPath)
                    identifiers.append(node)
            
            if len(whats):
                # Run it
                print 'Getting sizes and frequencies...'
                (retValuesOk, retValuesKo) = td.distribute(identifiers, whats, self.functionGetSizeFreq, numprocessors, numnodes)
                td.showKoAll(retValuesKo)
                for retValueOk in sorted(retValuesOk):
                    (node, absPath, refFreq, size, beamIndex) = retValueOk
                    absPaths.append(absPath)
                    nodes.append(node)
                    refFreqs.append(refFreq)
                    sizes.append(size)
                    beamIndexes.append(beamIndex)
        
        if len(absPaths) == 0:
            print 'No SBs found in node-paths.'
        else:
            # Write the reference file
            if os.path.isfile(output):
                os.system('rm ' + output)
            referenceFile = ReferenceFile(output, None, absPaths, refFreqs, sizes, nodes, beamIndexes)
            referenceFile.write()    
            print output + ' correctly created!'
