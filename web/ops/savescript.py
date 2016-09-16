import cherrypy
from cherrypy.lib import httpauth
import json 
import os

class SaveScript:
    @cherrypy.expose
    def default(self, **args):
        fname = args['fname']    
        script = args['script']
        
        print fname, script
        ah = httpauth.parseAuthorization(cherrypy.request.headers['authorization']) 
        userName = ah['username']
        
        jDict = {'status' : 'error','msg': 'File already exists'}

        user_dir = os.path.join('users', userName)
        try:#to create a directory for the user.
            os.mkdir(user_dir)
        except:
            #assume dir already exists.
            pass
        files = os.listdir(user_dir)
        if fname in files:
            #file already exists warn the user.
            return json.dumps(jDict)
        else:
            fname = os.path.join(user_dir, fname) #set the file path to userdir
        
        try: #write the script contents to the file.
            fp = open(fname, 'w')
            fp.write(script)
            fp.close()
            os.system('chmod a+x ' + fname)
            jDict['status'] = 'success'
            jDict['msg'] = os.path.abspath(fname)
            jDict['link'] = fname
        except e:
            jDict['msg'] = e.what()

        return json.dumps(jDict)
