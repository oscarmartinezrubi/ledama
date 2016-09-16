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
  
class LaunchCalibrateSA(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parset', 'a', 'Parset file')  
        options.add('model', 'm', 'Sky model file')
        options.add('execpath', 'e', 'Execution path', mandatory=False, helpmessage=', you can specify the path where the calibrate-stand-alone is executed')
        options.add('logspath', 'l', 'Logs path')
        options.add('options', 'o', 'Extra options', mandatory=False, helpmessage=', i.e. extra options to be used in the calibrate-stand-alone commands, use calibrate-stand-alone --help to see them. For the sourcedb and parmdb options you can specify a single file or a folder. If you specify a folder, this should contain the files to be used in each calibrate command (in this case the MS and the related parmdb or sourcedb file are matched by SB index). If you are typing this in a terminal you should specify them between "", for example: "-v -f -n --sourcedb myothersourcedbfolder"')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 16)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('delay', 'd', 'Execution delay',helpmessage=' in seconds between different processes in the same node.', default=1)  
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)   
        # the information
        information = 'Launch calibrate-stand-alone'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (absPath, msoptions) = what
        currentNode = utils.getHostName()   
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
        commandCSA = 'sleep ' + str(executionDelay) + ' ; source ' + self.initfile
        if self.execpath != '':
            commandCSA += ' ; mkdir -p ' + self.execpath + ' ; cd ' + self.execpath
        commandCSA += ' ; calibrate-stand-alone '
        if msoptions != None:
            commandCSA += msoptions + ' '
        commandCSA += absPath + ' ' + self.parsetFilePath + ' ' + self.modelFilePath
                
        if node != currentNode:
            commandCSA =  'ssh ' + node + " '" + commandCSA + "'"

        if self.query:
            return (absPath, commandCSA)
        else:
            td.execute(commandCSA, True) #we redirect output to log        
    
    def getSBDict(self, ff):
        ffDict = {}
        if os.path.isdir(ff):
            if ff.lower().count('all'):
                for ffElement in os.listdir(ff):
                    sbIndex = msoperations.getSBIndex(ffElement)
                    if sbIndex in ffDict:
                        raise Exception ('Duplicated SB: ' + ffElement + ' in ' + ff)
                    ffDict[sbIndex] = ff + '/' + ffElement
        elif os.path.isfile(ff):
            ffDict[ff] = ff
        else:
            raise Exception(ff + ' is not found!')        
        return ffDict
    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what[0]) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
    
    def process(self, reffile,  parset, model, execpath, logspath, options, numprocessors, numnodes, delay, query, initfile):
        self.query = query   
        self.delay = delay
        self.initfile = os.path.abspath(initfile)
        self.parsetFilePath = os.path.abspath(parset)
        self.modelFilePath = os.path.abspath(model)
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)
        self.execpath = os.path.abspath(execpath)
        self.options = options
        
        optionselements = None
        # get the dictionaries for possible sourcedb and parmdb
        sourcedbDict = None
        parmdbDict = None
        if self.options != '':
            optionselements = self.options.split(' ')
            if '--sourcedb' in optionselements:
                sourcedbDict = self.getSBDict(os.path.abspath(optionselements[optionselements.index('--sourcedb') + 1]))                        
            if '--parmdb' in optionselements:
                parmdbDict = self.getSBDict(os.path.abspath(optionselements[optionselements.index('--parmdb') + 1]))       
        # Create the distribution data
        whats = []
        referenceFile = ReferenceFile(reffile)
        for i in range(len(referenceFile.absPaths)):
            sbIndex = msoperations.getSBIndex(referenceFile.absPaths[i])
            msoptlements = None
            if optionselements != None:
                msoptlements = list(optionselements)
                if sourcedbDict != None:
                    if len(sourcedbDict) == 1:
                        msoptlements[msoptlements.index('--sourcedb') + 1] = sourcedbDict.values()[0]
                    else:
                        if sbIndex not in sourcedbDict:
                            print ('None sourcedb found for ' + referenceFile.absPaths[i] + ' in provided sourcedb folder')
                            return
                        msoptlements[msoptlements.index('--sourcedb') + 1] = sourcedbDict[sbIndex]
                if parmdbDict != None:
                    if len(parmdbDict) == 1:
                        msoptlements[msoptlements.index('--parmdb') + 1] = parmdbDict.values()[0]
                    else:
                        if sbIndex not in parmdbDict:
                            print ('None parmdb found for ' + referenceFile.absPaths[i] + ' in provided parmdb folder')
                            return
                        msoptlements[msoptlements.index('--parmdb') + 1] = parmdbDict[sbIndex]
                msoptlements = ' '.join(msoptlements)
            whats.append((referenceFile.absPaths[i], msoptlements))
        
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, whats, 
            self.function, numprocessors, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 