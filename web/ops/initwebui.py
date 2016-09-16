import json
import cherrypy
from ledama import utils
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.Connector import Connector, DEF_DBNAME, DEF_DBHOST
    
class InitWebUI:
    @cherrypy.expose
    def default(self):
        connection = Connector(DEF_DBNAME, utils.getUserName(), DEF_DBHOST).getConnection()
        qm = QueryManager()
        jDict = {}
        queryOptions = qm.getQueryOptions()
        jDict["QUERY_OPTIONS"] = queryOptions
        queryOptionsHeaderList = []
        queryOptionsInfos = []
        for queryOption in queryOptions:
            queryTable = qm.getQueryTable(queryOption)
            headerNames = queryTable.getHeaders()
            informations = queryTable.getInformations()
            queryOptionsInfos.append(queryTable.getDescription())
            queryOptionsHeader = {}
            queryOptionsHeader["name"] = queryOption
            queryOptionsHeader["header"] = ['Column', 'Description']
            dataRows = []
            for i in range(len(headerNames)):
                dataRows.append([headerNames[i], informations[i]])
            queryOptionsHeader["data"] = dataRows
            queryOptionsHeader["defaultin"] = True
            queryOptionsHeader["defhead"] = queryTable.getShowns()
            queryOptionsHeader["helptext"] = 'Select the columns to be shown'
            queryOptionsHeaderList.append(queryOptionsHeader)
        jDict["QUERY_OPTIONS_INFO"] = queryOptionsInfos
        jDict["QUERY_OPTIONS_HEADERS"] = queryOptionsHeaderList
        querySelectionOptionsList = []
        for querySelectionOption in qm.getQuerySelectionOptions():
            # In this case data type and packing type can be done with the filters
            (colHeader,dataRows,defaultIn,helpText) = qm.getQuerySelectionOptionParameters(querySelectionOption, connection)
            querySelectionOptionDict = {}
            querySelectionOptionDict["name"] = querySelectionOption
            querySelectionOptionDict["header"] = colHeader
            querySelectionOptionDict["data"] = dataRows
            querySelectionOptionDict["defaultin"] = defaultIn
            querySelectionOptionDict["helptext"] = helpText
            querySelectionOptionsList.append(querySelectionOptionDict)
        jDict["QUERY_SELECTION_OPTIONS"] = querySelectionOptionsList
        connection.close()
        return json.dumps(jDict)
