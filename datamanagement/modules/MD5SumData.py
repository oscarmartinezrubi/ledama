################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile

# Return a string with the concatenation of the md5sum of the contained files. 
# This method is recursive
def md5sumFolder(folderPath):
    for f in sorted(os.listdir(folderPath), key=str.lower):
        absfpath = folderPath + '/' + f
        if os.path.isdir(absfpath):
            md5sumFolder(absfpath)
        else:
            (md5out, md5err) = td.execute('md5sum ' + absfpath)
            if md5err != '':
                raise Exception(md5err[:-1])
            print md5out

class MD5SumData(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('output', 'o', 'Output file name')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 4)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('initfile', 'f', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        
        # the information
        information = 'Create a md5sum of the data pointed by a RefFile.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    # Function used for the tasksdistributor
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        (md5out, md5err) = td.execute("ssh " + node + " ' source " + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + md5sumFolder.__name__ + '(\\\"' + absPath +'\\\")"' + "'")
        if md5err != '':
            raise Exception(md5err[:-1])
        return (node, absPath, md5out)

    def process(self, reffile, output, numprocessors, numnodes, initfile):
        self.initfile = initfile
        referenceFile = ReferenceFile(reffile)
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, referenceFile.absPaths, self.function, numprocessors, numnodes)
        o=open(output,"w")
        for retValueOk in sorted(retValuesOk):
            o.write(' '.join(retValueOk) + '\n') 
        o.close()
        print 'Check output file: ' + output
        td.showKoAll(retValuesKo)