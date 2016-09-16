import json
import cherrypy
from cherrypy.lib import httpauth 
from ledama.leddb.Naming import GAINMOVIE_KEY, FILEPATH, HOST, GAINMOVIE, ID
from ledama.leddb.Connector import Connector
from ledama.leddb.query.QueryManager import QueryManager

TIMEOUT = 60

class GetPlotMovieCommand:
    @cherrypy.expose
    def default(self, **args):
        rdict = {'status' : 'error','msg': 'Some error happened!'}
        # Get data from fields
        selectedValue = args["sv"]
        
        names = [GAINMOVIE+ID,FILEPATH, HOST]
        qm = QueryManager()
        qc = qm.initConditions()
        qm.addCondition(GAINMOVIE_KEY, qc, GAINMOVIE+ID, int(selectedValue), operator='=')
        
        (query, queryDict) = qm.getQuery(GAINMOVIE_KEY, qc, names)
                
        connection = Connector().getConnection()
        cursor = connection.cursor()
        qm.executeQuery(connection,cursor,query,queryDict,False,TIMEOUT)
        
        (gainMovieId,moviepath, host) = cursor.fetchone()
        connection.close()
        
        rdict['status'] = 'success'
        rdict['msg'] = 'mplayer /net/' + host + moviepath
        
        return json.dumps(rdict) 
