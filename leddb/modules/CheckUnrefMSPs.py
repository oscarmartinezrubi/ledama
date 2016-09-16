################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import nodeoperations
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps

# EXTRA METHOD OUT OF THE CLASS, this necessary for ssh callings of such method without needing to create the instance 
def functionForSSH(parentPath, exclude, include, dbname, dbuser, dbhost):
    if exclude == '':
        exludePatterns = []
    else:
        exludePatterns = exclude.split(',')
    if include == '':
        includePatterns = []
    else:
        includePatterns = include.split(',')
    hostName = utils.getHostName()

    # Get the bands under this location in the leddb
    connection = Connector(dbname, dbuser, dbhost).getConnection()
    rows = LEDDBOps.select(connection, MSP, {HOST:hostName, PARENTPATH:(parentPath,'~')}, [PARENTPATH,NAME])
    connection.close()
    leddbBands = []  
    for row in rows:
        leddbBands.append(row[0] + '/' + row[1])
            
    # Get from direct (and recursive) analysis of the path
    foundAbsPaths = nodeoperations.getMSsFromPath(parentPath)
    
    for foundAbsPath in foundAbsPaths:
        unreferenced = True
        for i in range(len(leddbBands)):
            if foundAbsPath == leddbBands[i]:
                unreferenced = False
                break
        if unreferenced:
            show = True
            for exludePattern in exludePatterns:
                if foundAbsPath.count(exludePattern):
                    show = False
                    break
            if show:
                show = False
                if len(includePatterns):
                    for includePattern in includePatterns:
                        if foundAbsPath.count(includePattern):
                            show = True
                            break
                else:
                    show = True
                if show:
                    print str(msoperations.getSize(foundAbsPath)) + '\t' + foundAbsPath
                    
class CheckUnrefMSPs(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('path','i','Paths to check',helpmessage=' in each node (if multiple, comma-separated)', default='/data1/users/lofareor,/data2/users/lofareor,/data3/users/lofareor')
        options.add('output','o','Output RefFile', helpmessage=', the unreferenced MSPs will be written in a refFile', default=utils.getHome(userName) + "/unreferenced" + ReferenceFile.EXTENSION)
        options.add('exclude','e','Exclude patterns', mandatory=False, helpmessage=' (if multiple, comma-separated), paths containing these patterns will be excluded')
        options.add('include','c','Include patterns', mandatory=False, helpmessage=' (if multiple, comma-separated), only paths containing one of these patterns will be included')
        options.add('nodes','s','Nodes', helpmessage=' where we want to search the data. For example if we want to use the node001 and from the node010 to  node020 (both included) we should use node1,10-20', default='001-080')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 4)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 80)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'checks for MSPs that are not in the LEDDB.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # task to distribute with the task distributor     
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (absPath, exclude, include, dbname, dbuser, dbhost) = what
        currentNode = utils.getHostName()
        remoteCommand = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + functionForSSH.__name__ + '(\\\"' + absPath +'\\\",\\\"' + exclude +'\\\",\\\"' + include +'\\\",\\\"' + dbname +'\\\",\\\"' + dbuser +'\\\",\\\"' + dbhost +'\\\")"'
        if node != currentNode:
            remoteCommand =  'ssh ' + node + " '" + remoteCommand + "'"
        (out, err) = td.execute(remoteCommand)     
        if err != '':
            raise Exception(err)
        absPaths = []
        sizes = []
        beamIndexes = []
        for mes in out.split('\n'):
            if mes != '':
                (size, absPath) = mes.split('\t')
                absPaths.append(absPath)
                sizes.append(int(size))
                beamIndexes.append(msoperations.getBeamIndex(absPath))
        return (node, absPaths, sizes, beamIndexes)


    def process(self, path, output, exclude, include, nodes, numprocessors, numnodes, dbname, dbuser, dbhost, initfile):
        self.initfile = initfile
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS)+ ' can execute this code'
            return
        # Check for already existing output files
        if os.path.isfile(output):
            print 'Error: ' + output + ' already exists'
            return
        # We prepare a list of nodes-paths to be checked in each node
        cnodes = []
        cwhats = []
        for pathToCheck in path.split(','):
            for node in utils.getNodes(nodes):
                cnodes.append(node)
                cwhats.append((pathToCheck, exclude, include, dbname, dbuser, dbhost))
        # Get from all the nodes a list of the detected bands
        (retValuesOk,retValuesKo) = td.distribute(cnodes, cwhats, self.function, numprocessors, numnodes)
        td.showKoAll(retValuesKo)
        absPaths = []
        sizes = []
        nodes = []
        refFreqs = []
        beamIndexes = []
        for (node, nodeAbsPaths, nodeSizes, nodeBeamIndexes) in retValuesOk:
            for i in range(len(nodeAbsPaths)):
                absPaths.append(nodeAbsPaths[i])
                nodes.append(node)
                sizes.append(nodeSizes[i])
                refFreqs.append('?')
                beamIndexes.append(nodeBeamIndexes[i])
        ReferenceFile(output, None, absPaths, refFreqs, sizes, nodes, beamIndexes).write()
        print 'Check generated ' + output
        return