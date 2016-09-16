import pam
import os.path

root_dir = os.path.abspath('.')
def checkpasswd(realm, username, password):
    return pam.authenticate(username, password)
