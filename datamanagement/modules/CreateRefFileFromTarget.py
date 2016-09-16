################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
import ledama.utils as utils
from ledama.datamanagement.modules.archive.fdt.GetFDTFileList import GetFDTFileList
from ledama import msoperations

class CreateRefFileFromTarget(LModule):
    def __init__(self,userName = None):
        options = LModuleOptions()
        options.add('store','s','Store',choice=(utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR))
        options.add('directory','i','Directory')
        options.add('output', 'o', 'Output RefFile')
        information = 'Create a RefFile from a directory in Target locations.'
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, store, directory, output):
        # Check for already existing output files
        if os.path.isfile(output):
            print 'WARNING: ' + output + ' already exists. It will be overwritten...'
        try:
            (sizes, absPaths) = GetFDTFileList().getFileList(store, utils.formatPath(directory))
            sortedIndexes = [i[0] for i in sorted(enumerate(absPaths), key=lambda x:x[1])]
            sizesSorted = []
            absPathsSorted = []
            refFreqs=[]
            nodes=[]
            beamIndexes=[]
            for index in sortedIndexes:
                absPathsSorted.append(absPaths[index])
                sizesSorted.append(sizes[index])
                nodes.append(store)
                refFreqs.append('?')
                beamIndexes.append(msoperations.getBeamIndex(absPaths[index]))
            # Write the reference file
            if os.path.isfile(output):
                os.system('rm ' + output)
            referenceFile = ReferenceFile(output, None, absPathsSorted, refFreqs, sizesSorted, nodes, beamIndexes)
            referenceFile.write()    
            print output + ' correctly created!'   
        except Exception, e:
            print str(e)