################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os,sys
import ledama
from ledama import utils
from ledama import config as lconfig

class LModuleLoader:
    """LModule loader"""

    # Initialize the MSP all None
    def __init__(self, userName = None):

        if userName == None:
            userName = utils.getUserName()
        self.username = userName
        
        # Define the module directories within the LEDAMA source code directory    
        if self.username in lconfig.FULL_ACCESS_USERS:
            MODULES_PATHS = lconfig.ENABLED_LMODULES_PATHS_FULL_ACCESS_USERS
        elif self.username in lconfig.WEB_USERS:
            MODULES_PATHS = lconfig.ENABLED_LMODULES_PATHS_WEB_USERS
        else:
            MODULES_PATHS = lconfig.ENABLED_LMODULES_PATHS_USERS
        
        # From the MODULES_PATHS we get the modules names and paths of all the LModules that we can run with this executable script
        self.ledamaModulesPaths = []
        self.ledamaModulesNames = []
        self.ledamaModulesNamesLower = []
        self.ledamaModulesAbsPaths = []
        self.ledamaModulesTypes = []
        for modulePath in MODULES_PATHS:
            for element in os.listdir(ledama.__path__[0] + modulePath):
                if os.path.isfile(ledama.__path__[0] + modulePath + '/' + element) and element.endswith('.py') and not element.startswith('__'):
                    self.ledamaModulesNames.append(element.replace('.py',''))
                    self.ledamaModulesNamesLower.append(self.ledamaModulesNames[-1].lower())
                    self.ledamaModulesAbsPaths.append(ledama.__path__[0] + modulePath + '/' + element)
                    self.ledamaModulesPaths.append('ledama' + (modulePath + '/' + element).replace(ledama.__path__[0],'').replace('/','.').replace('.py',''))
                    self.ledamaModulesTypes.append(modulePath)

    # Get the list of the current LEDAMA modules name
    def getLModulesNames(self):
        return self.ledamaModulesNames
    
    # Check if a module name is correct
    def isValid(self, moduleName):
        return moduleName.lower() in self.ledamaModulesNamesLower
    
    # Get an instance of the module
    def getInstance(self, moduleName):
        # We get the related module path (we need it to load the module!)
        modIndex = self.ledamaModulesNamesLower.index(moduleName.lower())
        modulePath = self.ledamaModulesPaths[modIndex] 
        # We import the module
        exec 'import ' + modulePath
        # We create the related module instance. For this we assume that the module 
        # contains a class definition with the same class name than the module. 
        # We also assume the init method does not need any argument
        if self.username != None:
            return getattr(sys.modules[modulePath], self.ledamaModulesNames[modIndex])(self.username)
        else:
            return getattr(sys.modules[modulePath], self.ledamaModulesNames[modIndex])()
    
    def getAbsPath(self, moduleName):
        return self.ledamaModulesAbsPaths[self.ledamaModulesNamesLower.index(moduleName.lower())]
    
    def getType(self, moduleName):
        moduleType = self.ledamaModulesTypes[self.ledamaModulesNamesLower.index(moduleName.lower())]
        return moduleType.replace('/modules','').replace('/',' ').replace('data', 'data ').strip().upper().replace('MANAGEMENT PIPELINE','PROCESSING')