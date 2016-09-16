#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils

class LModule:
    """LEDAMA Module. Abstract class"""
    def __init__(self, options, information, showTimeInfo = True):
        """ Initialize the LModule, the arguments are:
             - options is a LModuleOption instance 
             - information is a string which describes the LModule
             - showTimeInfo is a boolean used to know if, when executing, 
               we should show timing info"""
        self.options = options
        self.information = information
        self.showTimeInfo = showTimeInfo
    
    def getOptions(self):
        """ Gets the options"""
        return self.options
    
    def getInformation(self):
        """ Gets the information message """
        return self.information
    
    def showTime(self):
        """ Gets showTimeInfo"""
        return self.showTimeInfo

    def process(self, *args):
        """ The main method of the module (TO BE OVERWRITTEN IN EACH CASE).
            When overwriting this method the arguments order must be the same 
            than when specifying them in the LModuleOptions instance"""
        return
    
    def function(self, *args):
        """This is a function that some modules may use for distribute tasks.
           (TO BE OVERWRITTEN IN EACH CASE)"""
        return
    
    def getCommand(self, currentArguments):
        """ Get the command for command-line execution"""
        if currentArguments != None:
            scriptArguments = ''
            argsLetter = self.options.getLetters()
            argsDescription = self.options.getDescriptions()
            argsMandatories = self.options.getMandatories()
            for i in range(len(argsLetter)):
                if type(currentArguments[i]) == bool:
                    if currentArguments[i]:
                        scriptArguments += ' -' + argsLetter[i] + ' '
                else:
                    if type(currentArguments[i]) == str: 
                        if currentArguments[i].count(' '):
                            # If we detect spaces in the argument, we try to use special chars to specify it in the command line
                            if currentArguments[i].count('"') and not currentArguments[i].count("'"):
                                scriptArguments += ' -' + argsLetter[i] + " '" + str(currentArguments[i]) + "' "
                            elif not currentArguments[i].count('"') and currentArguments[i].count("'"):
                                scriptArguments += ' -' + argsLetter[i] + ' "' + str(currentArguments[i]) + '" '
                            elif not currentArguments[i].count('"') and not currentArguments[i].count("'"):
                                scriptArguments += ' -' + argsLetter[i] + ' "' + str(currentArguments[i]) + '" '
                            else:
                                raise Exception(argsDescription[i] + ' error specifying command line argument')
                        elif currentArguments[i] != '':
                            # It is a string without white-spaces    
                            scriptArguments += ' -' + argsLetter[i] + ' ' + str(currentArguments[i]) + ' '
                        elif argsMandatories[i]:
                            raise Exception(argsDescription[i] + ' error specifying command line argument')
                    else:
                        # It is an integer,float...
                        scriptArguments += ' -' + argsLetter[i] + ' ' + str(currentArguments[i]) + ' '
            
            # We have to find the path of the related python script
            scriptPath = utils.SOFTWAREPATH  + 'ledama/ExecuteLModule'
            return ("python " + scriptPath + ' ' + self.__class__.__name__ + scriptArguments)
        else:
            raise Exception('Error: none provided arguments')
