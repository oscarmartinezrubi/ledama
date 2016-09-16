################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.datamanagement.modules.pipeline.CreateVDSFiles import CreateVDSFiles
from ledama.datamanagement.modules.pipeline.CreateVDSFiles import EXTENSION as EXTENSION_VDS
    
# File extensions
FILE_PREFIX = 'calc'
EXTENSION = '.gds'

# In option1 the number of GDS files to be created are the number of SBs per 
# each node, i.e. if we have 248 SBs, and in each node of (11 to 73) we have 
# 4 of them we would them have 4 GDS files. The first GDS will contain the 
# first SBs of each node, the second GDS the seconds ones and so on
GDS_CREATION_OPTION_1_KEYWORD = '1_SB_PER_NODE_IN_EACH_GDS'

# In the option2 the number of GDS files is the number of beams, the first GDS 
# contains the first beam and so on.
GDS_CREATION_OPTION_2_KEYWORD = 'GDS_PER_BEAM'

NO_KEYWORD = 'NO_KEYWORD'

GDS_KEYWORDS = [NO_KEYWORD, GDS_CREATION_OPTION_1_KEYWORD, GDS_CREATION_OPTION_2_KEYWORD]

EXPECTED_ERROR_MESSAGE = 'log4cplus:WARN Property configuration file "combinevds.log_prop" not found.\nlog4cplus:WARN Using basic logging configuration.\n'

class CreateGDSFiles(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('vds', 'v', 'Input VDS files folder')
        options.add('subbandskey', 'k', 'SubBands key', default=NO_KEYWORD, choice=GDS_KEYWORDS)
        options.add('subbands', 'b', 'SubBands', mandatory = False, helpmessage=' to use for each GDS file (only used if ' + NO_KEYWORD + ' is selected). For example if we want to create a GDS file with the SBs 0,1,2,4 and 8 we should use 0-4,8. If user wants to create multiple GDS files,split them by /, for example 0-3,6-10/4,5. If we do not specify it, a single GDS with all the SBs will be created')
        options.add('gds', 'g', 'Output GDS files folder', mandatory = False,helpmessage=', if not specified the same VDS path will be used')
        options.add('basename', 'n', 'GDS file base name',helpmessage=', the created GDS files will be named [basename]_XXX.gds where XXX is a number', default=FILE_PREFIX)
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        # the information
        information = 'Create GDS files'
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
        # We need an instance of CreateVDSFiles
        self.createVDSFiles = CreateVDSFiles()

    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (vdsFilesForGDS, outPath) = what
        commandGds = "combinevds " + outPath + " " + " ".join(vdsFilesForGDS)
        (makegdsOut, makegdsErr) = td.execute(commandGds)
        if makegdsErr != '' and makegdsErr != EXPECTED_ERROR_MESSAGE:
            raise Exception(makegdsErr[:-1])
        elif makegdsOut != '':
            raise Exception(makegdsOut[:-1])
        return  (identifier,outPath)
        
    # Get from the referenceFile and bandsToUseList the list of indexes according to option 0
    def getOption0IndexesLists(self, refFile, bandsToUseList):
        indexesToUseList = []
        for bandsToUse in bandsToUseList:
            indexesToUse = []
            for i in range(len(refFile.absPaths)):
                if msoperations.getSBIndex(refFile.absPaths[i]) in bandsToUse:
                    indexesToUse.append(i)
            indexesToUseList.append(indexesToUse)
        return indexesToUseList
    
    def getOption0AllIndexesLists(self, refFile):
        indexesToUse = []
        for i in range(len(refFile.absPaths)):
            indexesToUse.append(i)
        return [indexesToUse,]    
        
    # Get from the reference file the lists of indexes to use according to option1 
    # keyword
    def getOption1IndexesLists(self, refFile):
        indexesDictionary = utils.getIndexesDictionary(refFile.nodes)
        indexesToUseList = []
        maxNumMSPsPerNode = 0
        for node in indexesDictionary:
            num = len(indexesDictionary[node])
            if num > maxNumMSPsPerNode:
                maxNumMSPsPerNode = num
                
        numGDSFiles = maxNumMSPsPerNode
        for i in range(numGDSFiles):
            indexesToUseList.append([])
        
        for node in indexesDictionary:
            indexesInThisNode = indexesDictionary[node]
            for i in range(len(indexesInThisNode)):
                indexesToUseList[i].append(indexesInThisNode[i])
        return indexesToUseList
    
    # Get from the reference file the lists of indexes to use according to option2 
    # keyword
    def getOption2IndexesLists(self, refFile):
        indexesDictionary = utils.getIndexesDictionary(refFile.beamIndexes)
        indexesToUseList = []        
        for beamIndex in indexesDictionary:
            indexesToUseList.append(indexesDictionary[beamIndex])
        return indexesToUseList

    def process(self, reffile, vds, subbandskey, subbands, gds, basename, numprocessors):
        # Load the reference file, check it works
        referenceFile = ReferenceFile(reffile)
        # Get the node name to be used in the tasks dist.
        nodeName = utils.getHostName()
        vds = os.path.abspath(vds)
        # Get GDS output path
        if gds == '':
            gds = vds
        else:
            gds = os.path.abspath(gds)
        # Load the VDS files and store them in vdsFiles dictionary
        vdsFiles = {}
        # we create a dictionary for possible missing VDs files
        if not os.path.isdir(vds):
            print 'Directory not found: ' + vds
            return
        for i in range(len(referenceFile.absPaths)):
            relVDS = vds + '/' + self.createVDSFiles.getVDSFileName(referenceFile.absPaths[i])
            if os.path.isfile(relVDS):
                vdsFiles[i] = relVDS
        if not len(vdsFiles):
            print 'No VDS files found in ' + vds
            return
        
        # Depending if a special option the way of getting the indexes will change
        if subbandskey == GDS_CREATION_OPTION_1_KEYWORD:
            indexesToUseList = self.getOption1IndexesLists(referenceFile)
        elif subbandskey == GDS_CREATION_OPTION_2_KEYWORD:
            indexesToUseList = self.getOption2IndexesLists(referenceFile)
        else:
            if subbands == '':
                indexesToUseList = self.getOption0AllIndexesLists(referenceFile)
            else:
                # None special options, let's get them using the subbands
                bandsToUseList = []
                for sbs in subbands.split('/'):
                    bandsToUseList.append(utils.getElements(sbs))
                indexesToUseList = self.getOption0IndexesLists(referenceFile, bandsToUseList)
            
        # The variables required for the tasks distibributor
        nodes = []
        whats = []  
        for i in range(len(indexesToUseList)):
            # For each required GDS file we get its required indexes, these indexes
            # are related to entries in the refFile
            indexesToUse = indexesToUseList[i]
            # Add the node to the nodes, this is a dummy operation
            nodes.append(nodeName)
            
            # Create the VDS files list that will contain the VDS files fro this GDS
            vdsFilesForGDS = []
            for index in indexesToUse:
                if index in vdsFiles:
                    vdsFilesForGDS.append(vdsFiles[index])
                else:
                    # This index is not in the vdsFile
                    if index >= len(referenceFile.absPaths):
                        print 'VDS file not found for index ' + str(index)
                    else:
                        print 'VDS file not found for ' + referenceFile.absPaths[index]
            if len(vdsFilesForGDS) == 1:
                outputGDS = gds + '/' + self.createVDSFiles.getVDSFileName(referenceFile.absPaths[index]).replace(EXTENSION_VDS, EXTENSION)
            elif len(indexesToUseList) == 1:
                outputGDS = gds + '/' + basename + EXTENSION
            else:
                outputGDS = gds + '/' + basename + ('%03d' % i) + EXTENSION
            whats.append((vdsFilesForGDS, outputGDS))
            
        # Create the ouptut path for the GDS if it does not exist
        os.system('mkdir -p ' + gds)
        (retValuesOk, retValuesKo) = td.distribute(nodes, whats, self.function, numprocessors, 1)
        td.showOk(retValuesOk) 
        td.showKoFirst(retValuesKo) 
