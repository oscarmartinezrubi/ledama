################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
import ledama.tasksdistributor as td

class GetFDTFileList(LModule):
    def __init__(self,userName = None):
        options = LModuleOptions()
        options.add('store','s','Store',choice=(utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR))
        options.add('directory','i','Directory',mandatory = False)
        options.add('output','o','Output FDTFileList',mandatory = False)
        # the information
        information = 'Write the FDTFileList file with the content of the specified directory in the store. If directory is not specified it shows the possible directories.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def getDirList(self, store):
        
        if store == utils.TARGET_F_EOR:
            port = 20002
        elif store == utils.TARGET_E_EOR:
            port = 20001
        else:
            raise Exception ('Only sthe following stores are supported: ' + ','.join((utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR)) )
            
        c = " java -jar " + lconfig.FDT_PATH + " -p " + str(port) + " -noupdates -silent -c lotar1.staging.lofar -pull -r -d . " + lconfig.TARGET_FDT_DIR_LIST_FILE
        td.execute(c)
        if os.path.isfile(lconfig.TARGET_FDT_DIR_LIST_FILE):
            availableSpace = ''
            dirList = []
            lines = open(lconfig.TARGET_FDT_DIR_LIST_FILE, 'r').read().split('\n')
            for line in lines:
                if line != '' and line.startswith('./'):
                    dirList.append(line.replace('./',''))
                elif (line.count('Available space:')):
                    availableSpace = line.replace('Available space:','')
            os.system('rm ' + lconfig.TARGET_FDT_DIR_LIST_FILE)
            return (dirList,availableSpace)
        else:
            raise Exception(lconfig.TARGET_FDT_DIR_LIST_FILE + ' count not get copied!')
            
    def getFileList(self, store, directory):
        if store == utils.TARGET_F_EOR:
            port = 20002
        elif store == utils.TARGET_E_EOR:
            port = 20001
        else:
            raise Exception ('Only the following stores are supported: ' + ','.join((utils.TARGET_E_OPS, utils.TARGET_E_EOR, utils.TARGET_F_EOR)) )
         
        tempDir = directory + '_TARGET_LIST'
        os.system('mkdir ' + tempDir)
        c = " java -jar " + lconfig.FDT_PATH + " -p " + str(port) + " -noupdates -silent -c lotar1.staging.lofar -pull -r -d " + tempDir + " " + directory + '/' + lconfig.TARGET_FDT_FILE_LIST_FILE
        td.execute(c)
        sizes = []
        paths = []
        if os.path.isfile(tempDir + '/' + lconfig.TARGET_FDT_FILE_LIST_FILE):
            filesDir = open(tempDir + '/' + lconfig.TARGET_FDT_FILE_LIST_FILE, 'r').read().split('\n')
            os.system('rm -r ' + tempDir)
            for f in filesDir:
                if f != '' and f.count(lconfig.TARGET_FDT_FILE_LIST_FILE) == 0:
                    (size, relativePath) = f.split()
                    sizes.append(int(size))
                    paths.append(directory + '/' + relativePath)   
        else:
            raise Exception(directory + '/' + lconfig.TARGET_FDT_FILE_LIST_FILE + ' count not get copied!')
        return (sizes, paths)
     
    def process(self, store, directory, output):
        directory = utils.formatPath(directory)
        if directory == '':
            try:
                (dirList,availableSpace) = self.getDirList(store)
                if len(dirList):
                    print ('Directories found in target-out-eor:')
                for direc in dirList:
                    print (direc)
                print ('Available Space: ' + str(availableSpace))
            except Exception, e:
                print (str(e))        
        else:
            if output == '':
                print ('ERROR: output file needs to be specified!')
                return
            try:
                (sizes, paths) = self.getFileList(store, directory)
                outFile = open(output, 'wb')
                for i in range(len(sizes)):
                    outFile.write(str(sizes[i]) + ' ' + paths[i] + '\n')
                outFile.close()
            except Exception, e:
                print (str(e))
            
            