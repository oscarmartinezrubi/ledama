################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama import utils
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.datamanagement.modules.pipeline.CreateAWImagerParsetFiles import CreateAWImagerParsetFiles

class LaunchAWImager(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parsets', 'p', 'Input AWImager parset files folder')
        options.add('output', 'o', 'Output path', helpmessage=', you must specify the path where the imager is executed and the files produced will be stored (names for such files are defined in the parsets). Please use a path within images directory, example: /dataX/users/lofareor/images/LXXXX_XXXXX_XXX')
        options.add('oreffile', 'r', 'Output RefFile', mandatory=False, helpmessage=', this file will contain the locations of the generated ".fits" folders. This RefFile should not be used in the rest of LEDAMA modules.')
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)         
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        
        # the information
        information = 'Launch AWImager'
        
        # We need an instance of CreateCASAImagerParsetFiles
        self.createAWImagerParsetFiles = CreateAWImagerParsetFiles()
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (absPath, parset) = what
        
        currentNode = utils.getHostName()
        # Define commands
        commandAW = 'source ' + self.initfile+ ' ; mkdir -p ' + self.output + ' ; cd ' + self.output + ' ; awimager ' + parset
        if node != currentNode:
            commandAW =  "ssh " + node + " '" + commandAW + "'"
        if self.query:
            return (absPath, commandAW)
        else:
            td.execute(commandAW, True) #we redirect output to log

    def getImageName(self, inputfile):
        for line in (open(inputfile, "r")).read().split('\n'):
            if line.count('image='):
                return line.replace('image=','').strip()
        return None
    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what[0]) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
            
    def process(self, reffile, parsets, output, oreffile, numnodes, logspath, query, initfile): 
        self.query = query   
        self.initfile = os.path.abspath(initfile)
        parsets = os.path.abspath(parsets)
        self.output = utils.formatPath(output)
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)
        
        referenceFile = ReferenceFile(reffile)
        
        # we create a dictionary for possible missing VDs files
        if not os.path.isdir(parsets):
            print 'Directory not found: ' + parsets
            return
        
        refFreqs=[]
        sizes=[]
        absPaths=[]
        beamIndexes=[]
        nodes = []
        whats = []
        for i in range(len(referenceFile.absPaths)):
            relParset = parsets + '/' + self.createAWImagerParsetFiles.getAWImagerParsetFileName(referenceFile.absPaths[i])
            if os.path.isfile(relParset):
                whats.append((referenceFile.absPaths[i],relParset))
                nodes.append(referenceFile.nodes[i])
                refFreqs.append(None)
                sizes.append(0)
                fitsName = self.getImageName(relParset)
                if fitsName == None:
                    print 'Image name could not be extracted from ' + relParset
                    return  
                absPaths.append(self.output + '/' + fitsName)
                beamIndexes.append(0)
                
        if not len(whats):
            print 'No parset files found in ' + parsets
            return
        elif len(whats) != len(referenceFile.absPaths):
            print 'WARNING: not all parsets were found!'
           
        if oreffile != '':
            if os.path.isfile(oreffile):
                os.system('rm ' + oreffile)
            outreferenceFile = ReferenceFile(oreffile, None, absPaths, refFreqs, sizes, nodes, beamIndexes)
            outreferenceFile.write()   

        # Run it
        (retValuesOk, retValuesKo) = td.distribute(nodes, whats, 
            self.function, 1, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 