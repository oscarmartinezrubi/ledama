#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
class LModuleOptions:
    """LEDAMA Options"""
    def __init__(self,):
        """ Initialize all the variables with empty lists"""
        self.names = []
        self.descriptions = []
        self.helpmessages = []
        self.letters = []
        self.mandatories = []
        self.defaults = []
        self.choices = []
        
    def add(self, name, letter, description, helpmessage = '', default = '', mandatory = True,  choice = None):
        """ Add an option """
        self.names.append(name)
        self.descriptions.append(description)
        self.helpmessages.append(helpmessage)
        self.letters.append(letter)
        if default != '':
            # If default values is provided, the option will always be provided,
            # we do not need to check if it is provided
            self.mandatories.append(False)
        else:
            self.mandatories.append(mandatory)
        self.defaults.append(default)
        self.choices.append(choice)
    
    def modifyOption(self, name, letter = None, description = None, helpmessage = None, default = None, mandatory = None,  choice = None):
        """ Modify an option given its name, if the given name is not a already
            introduced option an Exception will be raised"""
        index = self.names.index(name)
        if letter != None:
            self.letters[index] = letter
        if description != None:
            self.descriptions[index] = description
        if helpmessage != None:
            self.helpmessages[index] = helpmessage
        if default != None:
            self.defaults[index] = default
        if mandatory != None:
            if self.defaults[index] != '':
                self.mandatories[index] =  False
            else:
                self.mandatories[index] = mandatory
        if choice != None:
            self.choices[index] = choice
    
    def getNames(self):
        """ Get the names"""
        return self.names
    
    def getDescriptions(self):
        """ Get the descriptions"""
        return self.descriptions
    
    def getHelpMessages(self):
        """ Get the help messages"""
        return self.helpmessages
    
    def getLetters(self):
        """ get the letters"""
        return self.letters
    
    def getMandatories(self):
        """ Get the mandatories"""
        return self.mandatories
    
    def getDefaults(self):
        """ Get the defaults"""
        return self.defaults
    
    def getChoices(self):
        """ Get the choices"""
        return self.choices 
    
    def getTypes(self):
        """ Get the types (from the defaults)"""
        types = []
        for default in self.defaults:
            types.append(type(default))
        return types