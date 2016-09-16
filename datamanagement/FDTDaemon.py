#!/usr/bin/env python
import sys, os
from ledama.daemon import Daemon
import ledama.utils as utils
import ledama.config as lconfig

def getPidFilePath(hostName):
    return lconfig.NODE_MONITOR_FOLDER +  hostName + '.fdt.pid'

class FDTDaemon(Daemon):
    def run(self):
        os.system('java -jar ' + lconfig.FDT_PATH)
    def killFDT(self):
        os.system("ps aux | grep fdt.jar | grep java | awk '{print $2}' | xargs -I proc kill -9 proc")
            
if __name__ == "__main__":
    daemon = FDTDaemon(getPidFilePath(utils.getHostName()))
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
            daemon.killFDT()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
