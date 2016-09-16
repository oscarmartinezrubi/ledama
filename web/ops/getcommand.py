import cherrypy
from cherrypy.lib import httpauth
import json 
from ledama.LModuleLoader import LModuleLoader    

class GetCommand:
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def default(self, **args):
        dict = cherrypy.request.json
        moduleName = dict['MODULE']    
        currentArguments = dict['ARGS']
        
        ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
        userName = ah['username']
        
        mod = LModuleLoader(userName).getInstance(moduleName)
        modOptions = mod.getOptions()
        argumentsDescs = modOptions.getDescriptions()
        argumentsTypes = modOptions.getTypes()
        argsMandatory = modOptions.getMandatories()
        
        jDict = {'status' : 'error','msg': ''}
        
        castArguments = []
        for i in range(len(currentArguments)):
            if argsMandatory[i] and currentArguments[i] == '':
                jDict['msg'] = argumentsDescs[i] + ' is mandatory!'
                return json.dumps(jDict)
            else:
                castArguments.append(argumentsTypes[i](currentArguments[i]))     
        c = ''
        try:
            c = mod.getCommand(tuple(castArguments))
        except Exception, e:
            jDict['msg'] = str(e)
            
        if c != '':
            jDict['msg'] = c
            jDict['status'] = 'success'
        return json.dumps(jDict)