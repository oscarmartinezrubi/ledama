################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import nodeoperations
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

def functionForSSH(absPaths, node, checkStr, forceStr):
    check = utils.booleanStringToBoolean(checkStr)
    force = utils.booleanStringToBoolean(forceStr)
    for absPath in absPaths:
        for ldsPath in sorted(nodeoperations.getLDSPaths(absPath)):
            contents = os.listdir(ldsPath)
            if len(contents) == 0:
                if check:
                    print '   ' + ldsPath
                else:
                    print '   ' + ldsPath + ' DELETED'
                    os.system('rm -rf ' + ldsPath)
            else:
                mss = nodeoperations.getMSsFromPath(ldsPath)
                if len(mss) == 0:
                    print '   ' + ldsPath + " do not contain MSs (but other data)"
                    if force:
                        print '   ' + ldsPath + ' FORCE-DELETED'
                        os.system('rm -rf ' + ldsPath)

class CleanEmptyLDSPaths(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('inputpath','i','Path[s]',default='/data1/users/lofareor,/data1/users/lofareor/pipeline,/data2/users/lofareor,/data2/users/lofareor/pipeline,/data3/users/lofareor,/data3/users/lofareor/pipeline,/data3/users/lofareor/backup,/data3/users/lofareor/backup/pipeline', helpmessage=' (if multiple, comma-separated no blank spaces)')
        options.add('nodes', 'u', 'Nodes', default = 'node001-080')
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 80)
        options.add('check', 'c', 'Check (not delete)', helpmessage = ', only shows the contents of the folder', default = False)
        options.add('force', 'f',  'Force', helpmessage = ', delete even if not empty', default = False)
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Delete empty LDSs paths'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
            
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        currentNode = utils.getHostName()
        commandClean = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + functionForSSH.__name__ + '(\\\"' + self.lpath +'\\\", \\\"' + node +'\\\", \\\"' + str(self.check) +'\\\", \\\"' + str(self.force) +'\\\")"'
        if node != currentNode:
            commandClean =  'ssh ' + node + " '" + commandClean + "'"
        (cleanout, cleanerr) = td.execute(commandClean)     
        if cleanerr != '':
            raise Exception(cleanerr)
        return (node, cleanout)

    def process(self, inputpath, nodes, numnodes, check, force, initfile):
        self.check = check
        self.force = force
        self.lpath = inputpath
        self.initfile = os.path.abspath(initfile)
        
        ns = utils.getNodes(nodes)
           
        (retValuesOk,retValuesKo) = td.distribute(ns, ns, self.function, 1, numnodes)
        td.showOk(retValuesOk, True)
        td.showKoAll(retValuesKo)