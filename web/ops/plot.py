import cherrypy
from cherrypy.lib import httpauth
import json, os
from getcommand import GetCommand
import ledama.utils as utils

TIME_FORMAT = "%Y_%m_%d_%H_%M_%S"

class Plot:
    @cherrypy.expose
    @cherrypy.tools.json_in()
    def default(self, **args):
        
        jDict = {'status' : 'error','msg': ''}
        ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
        userName = ah['username']
        outname = 'users/' + userName + '/' + utils.getCurrentTimeStamp(TIME_FORMAT) + 'tempImage.png'
        user_dir=os.path.join('users',userName)
        try:
            #to create a directory for the user.
            os.mkdir(user_dir)
        except:
            #assume dir already exists.
            pass
        try:
            command = json.loads(GetCommand().default(**args))['msg']

            if command.count('-o'):
                d = command.split()
                d[d.index('-o')+1] = outname
                command = ' '.join(d)
            else:
                command += '-o ' + outname
        
            # delete possible file with same name
            if os.path.isfile(outname):
                os.system('rm ' + outname)
            
            # Run the command
            os.system(command)
            
            if os.path.isfile(outname):
                jDict['msg'] = outname
                jDict['status'] = 'success'
            
        except Exception, e:
            jDict['msg'] = str(e)
            
        return json.dumps(jDict)
