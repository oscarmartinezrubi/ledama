################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import msoperations
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

# This Python module contains the code to compare two reffiles

class CompareData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffiles', 'i', 'Input RefFiles', helpmessage = ', specify the reffiles comma-separated')
        options.add('subbands', 'b', 'SB indexes', helpmessage = ', if provided only these subband indexes are considered', mandatory = False)

        # the information
        information = """Compare the contents of some refFiles. It checks using the SB index and the size.
        If only one reffile is given it only checks that the provided SBs are in the reffile and that they are not multiple.
        If multiple reffiles are given the first one is used as reference for the sizes"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def process(self, reffiles,subbands):
        
        # Open the refFile
        referenceFiles = []
        refFiles = reffiles.split(',')
        for reffile in refFiles:
            referenceFiles.append(ReferenceFile(reffile))
            
    
        sbs = None
        if subbands != '':
            sbs = utils.getElements(subbands)
        elif len(referenceFiles) == 1:
            print 'If only one reffile is given we need to know which SBs to use'
            return
    
        sbDict = {}
        
        for refFileIndex in range(len(referenceFiles)):
            for i in range(len(referenceFiles[refFileIndex].absPaths)):
                sbIndex = msoperations.getSBIndex(referenceFiles[refFileIndex].absPaths[i])
                if sbs == None or sbIndex in sbs:
                    if not sbIndex in sbDict:
                        sbDict[sbIndex] = []
                        for j in range(len(referenceFiles)):
                            sbDict[sbIndex].append([])
                    sbDict[sbIndex][refFileIndex].append(referenceFiles[refFileIndex].sizes[i])
        
        missing = []
        multiple = []
        errorsizes = []
        
        for refFileIndex in range(len(referenceFiles)):
            missing.append([])
            multiple.append([])
            errorsizes.append([])
                
        if sbs != None:
            for sbIndex in sbs:
                if sbIndex not in sbDict:
                    for refFileIndex in range(len(referenceFiles)):
                        missing[refFileIndex].append(str(sbIndex))
                    
        for sbIndex in sbDict:
            for refFileIndex in range(len(referenceFiles)):
                sizes = sbDict[sbIndex][refFileIndex]
                if len(sizes) == 0:
                    missing[refFileIndex].append(str(sbIndex))
                elif len(sizes) > 1:
                    multiple[refFileIndex].append(str(sbIndex))
                else:
                    if refFileIndex > 0:
                        if len(sbDict[sbIndex][0]) == 1 and sbDict[sbIndex][refFileIndex][0] != sbDict[sbIndex][0][0]:
                            errorsizes[refFileIndex].append(str(sbIndex)+'|'+str(sbDict[sbIndex][refFileIndex][0])+'|'+str(sbDict[sbIndex][0][0]))
        
        
        for refFileIndex in range(len(referenceFiles)):
            if len(missing[refFileIndex]) or len(multiple[refFileIndex]) or len(errorsizes[refFileIndex]):
                print 'Results for ' + refFiles[refFileIndex] + ':'
                if len(missing[refFileIndex]):
                    print '    Missing: ' + (','.join(missing[refFileIndex]))
                if len(multiple[refFileIndex]):
                    print '    Multiple: ' + (','.join(multiple[refFileIndex]))
                if len(errorsizes[refFileIndex]):
                    print '    Size errors: ' + (','.join(errorsizes[refFileIndex]))
            else:
                print 'Everything OK in ' + refFiles[refFileIndex]
