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
from ledama.datamanagement.modules.pipeline.CreateCASAImagerParsetFiles import CreateCASAImagerParsetFiles

class LaunchCASAImager(LModule):
    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parsets', 'p', 'Input CASAImager parset files folder')
        options.add('nodes', 'u', 'Nodes to run CASA imager', mandatory=False, helpmessage='. If not provided the nodes containing the data will be used')
        options.add('output', 'o', 'Output path', helpmessage=', you must specify the path where the imager is executed and the files produced will be stored (common file names for such files is defined in the parsets). Please use a path within images directory, example: /dataX/users/lofareor/images/LXXXX_XXXXX_XXX')
        options.add('oreffile', 'r', 'Output RefFile', mandatory=False, helpmessage=', this file will contain the locations of the generated ".fits" folders. This RefFile should not be used in the rest of LEDAMA modules.')
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)         
        options.add('logspath', 'l', 'Logs path')
        options.add('query', 'q', 'Query', helpmessage = '. It prints the commands without executing them', default = False)  
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        
        # the information
        information = 'Launch CASAImager'
        
        # We need an instance of CreateCASAImagerParsetFiles
        self.createCASAImagerParsetFiles = CreateCASAImagerParsetFiles()
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (absPath, parset) = what
        currentNode = utils.getHostName()
        # Define commands
        commandCASA = 'source ' + self.initfile+ ' ; mkdir -p ' + self.output + ' ; cd ' + self.output + ' ; casapy --nologger --nogui -c ' + parset
        if node != currentNode:
            commandCASA =  "ssh " + node + " '" + commandCASA + "'"
        if self.query:
            return (absPath, commandCASA)
        else:
            td.execute(commandCASA, True) #we redirect output to log
         

    def getImageName(self, inputfile):
        for line in (open(inputfile, "r")).read().split('\n'):
            if line.count('imagename="'):
                return line.split('"')[1].strip()
        return None
    
    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return msoperations.getMeasurementSetName(what[0]) + '_' + identifier + ('_C%03d_' % childIndex) + ('G%03d_' % grandChildIndex) + ('T%03d_' % taskIndex) + '.log'
    
    def addMS(self, msIndex, node):
        relParset = self.parsets + '/' + self.createCASAImagerParsetFiles.getCASAImagerParsetFileName(self.referenceFile.absPaths[msIndex])
        if os.path.isfile(relParset):
            # Check if image name can be extracted from parset
            imageName = self.getImageName(relParset)
            if imageName == None:
                raise Exception('Image name could not be extracted from ' + relParset)  
            # For task distributions
            self.whats.append((self.referenceFile.absPaths[msIndex], relParset))
            self.nodesToUse.append(node)
            # For the output reffile
            self.refFreqs.append(self.referenceFile.refFreqs[msIndex])
            self.sizes.append(0)
            self.absPaths.append(self.output + '/' + imageName + '.image')
            self.beamIndexes.append(msoperations.getBeamIndex(self.referenceFile.absPaths[msIndex]))
            
            
    def process(self, reffile, parsets, nodes, output, oreffile, numnodes, logspath, query, initfile):
        self.query = query   
        self.initfile = os.path.abspath(initfile)
        self.output = utils.formatPath(output)
        self.parsets = os.path.abspath(parsets)
        self.referenceFile = ReferenceFile(reffile)
        numMSs = len(self.referenceFile.absPaths)
        logsPath = None
        if (not self.query) and logspath != '':
            logsPath = os.path.abspath(logspath)
            
        # we create a dictionary for possible missing VDs files
        if not os.path.isdir(parsets):
            print 'Directory not found: ' + parsets
            return
        
        self.refFreqs=[]
        self.sizes=[]
        self.absPaths=[]
        self.beamIndexes=[]
        self.nodesToUse = []
        self.whats = []
        if nodes != '':
            # We use the provided nodes
            nodes = utils.getNodes(nodes)
            msIndexesPerNodes = utils.splitArray(range(numMSs), len(nodes))
            for i in range(len(nodes)):
                node = nodes[i]
                msIndexesPerNode = msIndexesPerNodes[i]
                for msIndex in msIndexesPerNode:
                    self.addMS(msIndex, node)
        else:
            # We use the nodes that contains the data
            for msIndex in range(numMSs):
                self.addMS(msIndex, self.referenceFile.nodes[msIndex])
        
        if not len(self.whats):
            print 'No parset files found in ' + self.parsets
            return
        elif len(self.whats) != numMSs:
            print 'WARNING: not all parsets were found!'
           
        if oreffile != '':
            if os.path.isfile(oreffile):
                os.system('rm ' + oreffile)
            outreferenceFile = ReferenceFile(oreffile, None, self.absPaths, self.refFreqs, self.sizes, self.nodesToUse, self.beamIndexes)
            outreferenceFile.write()   

        # Run it
        (retValuesOk, retValuesKo) = td.distribute(self.nodesToUse, self.whats, 
            self.function, 1, numnodes, logFolder = logsPath, getLogFileName = self.getLogFileName)
        td.showOk(retValuesOk)
        td.showKoFirst(retValuesKo) 