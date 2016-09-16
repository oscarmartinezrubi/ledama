################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import re,os
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import utils
from ledama import msoperations

class CreateRefFileFromRefFile(LModule):
    def __init__(self,userName = None):
        options = LModuleOptions()
        options.add('reffiles', 'i', 'Input RefFile[s] (if multiple, comma-separated without blank spaces)')
        options.add('output', 'o', 'Output RefFile')
        options.add('nodes', 's', 'Nodes', helpmessage = ' cut. For example, if we want to include data only in node001 and from the node010 to node020 (both included) we should use node1,10-20', mandatory = False)
        options.add('subbands', 'b', 'SubBands ', helpmessage = ' cut. For example, if we want to include only SBs whose indexes are 0,1,5,6,7 we should use 0,1,5-7', mandatory = False)
        options.add('exclude', 'e', 'Exclude patterns (if multiple, comma-separated)', helpmessage = ', paths containing these patterns will be excluded', mandatory = False)
        options.add('include', 'c', 'Include patterns (if multiple, comma-separated)', helpmessage = ', only paths containing one of these patterns will be included', mandatory = False)
        
        information = 'Create a RefFile from other RefFiles specifying some excluded or included patterns, SB indexes and nodes'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def process(self, reffiles, output, nodes, subbands, exclude, include):
        
        # Get the allowed nodes and subbands
        nodesToUse = None
        bandsToUse = None
        if nodes != '':
            nodesToUse = utils.getNodes(nodes)
        if subbands != '':
            bandsToUse = utils.getElements(subbands)          
    
        # Check for already existing output files
        if os.path.isfile(output):
            print 'ERROR: ' + output + ' already exists!'
            return
        
        if exclude == '':
            excludePatterns = []
        else:
            excludePatterns = exclude.split(',')
        if include == '':
            includePatterns = []
        else:
            includePatterns = include.split(',')
            
        oabsPaths = []
        onodes = []
        osizes = []
        orefFreqs = []
        obeamIndexes = []
        for refFile in reffiles.split(','):
            referenceFile = ReferenceFile(refFile)
            for i in range(len(referenceFile.absPaths)):
                absPath = referenceFile.absPaths[i]
                sbIndex= msoperations.getSBIndex(absPath)
                if ((bandsToUse == None) or (sbIndex in bandsToUse)) and ((nodesToUse == None) or (referenceFile.nodes[i] in nodesToUse)):
                    show = True
                    for excludePattern in excludePatterns:
                        if absPath.count(excludePattern):
                            show = False
                            break
                    if show:
                        show = False
                        if len(includePatterns):
                            for includePattern in includePatterns:
                                if absPath.count(includePattern):
                                    show = True
                                    break
                        else:
                            show = True
                        if show:
                            oabsPaths.append(absPath)
                            onodes.append(referenceFile.nodes[i])
                            osizes.append(referenceFile.sizes[i])
                            orefFreqs.append(referenceFile.refFreqs[i])
                            obeamIndexes.append(referenceFile.beamIndexes[i])
       
        # Check if we have some MSPs
        if len(oabsPaths) == 0:
            print 'ERROR: No SBs for selected options!'
            return
        
        # Write the output file
        ReferenceFile(output, None, oabsPaths, orefFreqs, osizes, onodes, obeamIndexes).write()
        print output + ' correctly created!'            