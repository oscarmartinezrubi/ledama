import cherrypy
from cherrypy.lib import httpauth
import json 
import ledama.utils as utils
from ledama.LModuleLoader import LModuleLoader    

class InitModuleUI:
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def default(self, moduleName = None):
        
        ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
        userName = ah['username']
        # Get data from fields
        #moduleName = args["module"]
        
        if moduleName == None:
            jDict = {}
            lModuleLoader = LModuleLoader(utils.getUserName())
            moduleNames = sorted(lModuleLoader.getLModulesNames())
            
            modules = {}
            for moduleName in moduleNames:
                moduleType = lModuleLoader.getType(moduleName)
                if modules.get(moduleType) == None:
                    modules[moduleType] = []
                modules.get(moduleType).append(moduleName)
            
            jDict["MODULES"] = modules
            return json.dumps(jDict)
        
        mod = LModuleLoader(userName).getInstance(moduleName)
        modOptions = mod.getOptions()

        argsName = modOptions.getNames()
        argsDescription = modOptions.getDescriptions()
        argsHelp = modOptions.getHelpMessages()
        argsMandatory = modOptions.getMandatories()
        argsDefault = modOptions.getDefaults()
        argsChoices = modOptions.getChoices()
        argsType = modOptions.getTypes()

        # We configure the options with the arguments configuration of the loaded module
        options = []
        for i in range(len(argsName)):
            optionType = 'TEXT'
            mandatory = ''
            default = ''
            if  str(argsDefault[i]) != '':
                default = ' [default is ' + str(argsDefault[i]) + ']'
            else:
                if argsMandatory[i]:
                    mandatory = ' [mandatory]'
                else:
                    mandatory = ' [optional]'
            
            optionHelp = argsDescription[i] + argsHelp[i] + mandatory + default
            
            if argsType[i] == bool:
                optionType = 'BOOLEAN'
                
            elif argsChoices != None and argsChoices[i] != None and (type(argsChoices[i]) in (list, tuple)):
                optionType = 'CHOICE'
                # In the case of choices, we send all the choices, the default 
                # should be the first one
                argsDefault[i] = argsChoices[i]
                optionHelp = argsDescription[i] + argsHelp[i] + mandatory + '. Choices are ' + ' | '.join(argsChoices[i]) + ' [default is ' + str(argsChoices[i][0]) + ']'
            
            optionDict = {}
            optionDict['NAME'] = argsDescription[i]
            optionDict['TYPE'] = optionType
            optionDict['DEFAULT'] = argsDefault[i]
            optionDict['HELP'] = optionHelp
            options.append(optionDict)
        
        
        jDict = {}
        jDict["MODULE"] = moduleName
        jDict["OPTIONS"] = options
        return json.dumps(jDict)
