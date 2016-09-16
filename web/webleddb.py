#!python
import os.path
import cherrypy
from auth import checkpasswd
from auth import root_dir
from ops.querier import Querier
from ops.getreffile import GetRefFile
from ops.getdiagfile import GetDiagFile
from ops.clustermonitor import ClusterMonitor
from ops.initwebui import InitWebUI
from ops.initmoduleui import InitModuleUI 
from ops.getcommand import GetCommand
from ops.savescript import SaveScript
from ops.getplotmoviecommand import GetPlotMovieCommand
from ops.plot import Plot

#imports for setting the custom session
import leddbsession
cherrypy.lib.sessions.LeddbSession = leddbsession.LeddbSession

class Root:
    @cherrypy.expose
    def monitor(self):
        return open("static/monitor.html")
    
    @cherrypy.expose
    def datamanager(self):
        return open("static/datamanager.html")

    @cherrypy.expose
    def index(self):
        return open("static/webleddb.html")

root = Root()
root.querier = Querier()
root.getreffile = GetRefFile()
root.getdiagfile = GetDiagFile()
root.clustermonitor = ClusterMonitor()
root.initwebui = InitWebUI()
root.initmoduleui = InitModuleUI()
root.getcommand = GetCommand()
root.savescript = SaveScript()
root.getplotmoviecommand = GetPlotMovieCommand()
root.plot = Plot()

cherrypy.quickstart(root, config=os.path.join(os.path.dirname(__file__), 'webleddb.conf'));
