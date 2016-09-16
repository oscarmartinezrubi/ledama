################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, random
from ledama import utils
from ledama import tasksdistributor as td
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.datamanagement.modules.pipeline.CreateGDSFiles import EXTENSION

class LaunchBBS(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        # Define the options
        options = LModuleOptions()
        options.add('gds', 'i', 'Input GDS files folder')
        options.add('numsimultgds', 'n', 'Simultaneous BBS executions', default = 1)
        options.add('parset', 'p', 'Parset file')  
        options.add('model', 'm', 'Sky model file')
        options.add('logspath', 'l', 'Logs path')
        options.add('dbnode', 'd', 'DB node', default='node079')
        options.add('dbname', 'b', 'DB name', default = self.userName)
        options.add('descfile', 'c', 'Cluster description file', default=utils.getDefClusDesc())
        options.add('delay', 'b', 'Execution delay',helpmessage=' in seconds between different processes in the same node.', default=1)
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)   
        # the information
        information = 'Launch BBS'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        gdsFile = what
        try:
            if self.aux: 
                executionDelay = 0
        except:
            # We will reach this point the first time this grandchildren executes
            # a function (because self.aux will not exist hence raising the exception). 
            # We only apply delay now (the rest of times this grandchild executes 
            # functions the delay will be implicitly given by previous executed function)
            self.aux = True
            executionDelay = grandChildIndex * self.delay
        # Define commands
        commandBBS = 'source ' + self.initfile + "; sleep " + str(executionDelay) + " ; calibrate -f --key " + node + '_' + gdsFile.replace(EXTENSION,'').split('/')[-1] + '_' +(self.randomIdMap[gdsFile]) + " --cluster-desc " + self.clusterDescriptionFilePath + "  --db " + self.dbnode + "  --db-name " + self.namedb + " --db-user postgres " + gdsFile + " " + self.parsetFilePath + "  " + self.modelFilePath + " " + self.logsPath
        if self.query:
            return (gdsFile, commandBBS)
        else:
            td.execute(commandBBS, True) #we redirect output to log       

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return os.path.basename(what) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'

    def process(self, gds,numsimultgds,parset,model,logspath,dbnode,dbname,descfile,delay,query,initfile):
        self.clusterDescriptionFilePath = descfile
        self.delay = delay
        self.parsetFilePath = parset
        self.modelFilePath = model
        self.dbnode = dbnode
        self.namedb = dbname
        self.query = query
        self.initfile = os.path.abspath(initfile)
        currentNode = utils.getHostName()
        self.logsPath = None
        if (not self.query) and logspath != '':
            self.logsPath = utils.formatPath(os.path.abspath(logspath))

        # Get the GDS files from the input path
        identifiers = []
        gdsFiles = []
        inputPath = utils.formatPath(gds)
        if os.path.isdir(inputPath):
            for gfile in sorted(os.listdir(inputPath), key=str.lower):
                if gfile.endswith(EXTENSION): 
                    gdsFiles.append(inputPath + '/' + gfile)
                    identifiers.append(currentNode)
        elif os.path.isfile(inputPath) and inputPath.endswith(EXTENSION):
            gdsFiles.append(inputPath)
            identifiers.append(currentNode)
        if len(gdsFiles) == 0:
            print 'No GDS files found in ' + inputPath
            return
       
        self.randomIdMap = {}
        # For each GDS we get a delay and a random number
        for i in range(len(gdsFiles)):
            self.randomIdMap[gdsFiles[i]] = ('%05d' % random.randrange(0, 10001))
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(identifiers, gdsFiles, 
            self.function, numsimultgds, 1, logFolder = self.logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 