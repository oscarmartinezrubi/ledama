#!/usr/bin/env python
import sys, time, os, json
import cPickle as pickle
from ledama import utils
from ledama import config as lconfig
from ledama.daemon import Daemon
from ledama.leddb import LEDDBOps
from ledama.leddb.Naming import HOST, NAME, STORAGEMON, NETMON, USAGEMON, GPUUSAGEMON
from ledama.leddb.Connector import Connector

def getPidFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER +  hostName + '.pid'
def getOutFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER +  hostName + '.out'
def getErrFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER +  hostName + '.err'


def getStorageFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER + hostName + '.storage'

def getUsageFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER + hostName + '.usage'

def getGPUUsageFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER + hostName + '.gpu'

def getNetFilePath(hostName):
	return lconfig.NODE_MONITOR_FOLDER + hostName + '.net'

class NodeMonitorDaemon(Daemon):
	
	def updateStorageStatus(self, data):
		linesToWrite = utils.getCurrentTimeStamp() + '\n'
		lines  = os.popen("df -B G | grep data").readlines()
		for line in lines:
			try:
				fields = line.split()
				mountpoint = fields[-1]
				use = int(fields[-2][:-1])
				available = fields[-3]
				linesToWrite += mountpoint + ' ' + str(use) + ' ' + str(available) + '\n'
			except:
				continue
		data[STORAGEMON] = linesToWrite

	def updateNetStatus(self, data):
		interval = 2
		lines = os.popen("cat /proc/net/dev | grep eth | cut -d ':' -f 1").read().split('\n')
		interfaces = []
		for line in lines:
			if line != '':
				interfaces.append(line.strip())
		
		lines = os.popen('cat /proc/net/dev | grep eth | cut -d \':\' -f 2 | awk \'{print $1, $9}\' ; sleep ' + str(interval) + ' ; cat /proc/net/dev | grep eth | cut -d \':\' -f 2 | awk \'{print $1, $9}\'').read().split('\n')
		currentTime = utils.getCurrentTimeStamp()
		linesToWrite = currentTime + '\n'
		for i in range(len(interfaces)):
			rx1 = float(lines[i].split()[0])
			tx1 = float(lines[i].split()[1])
			rx2 = float(lines[(len(interfaces)) + i].split()[0])
			tx2 = float(lines[(len(interfaces)) + i].split()[1])
			velrx = int((rx2 - rx1) / interval)
			veltx = int((tx2 - tx1) / interval)
			linesToWrite += str(interfaces[i]) + ' ' + str(velrx) + ' ' + str(veltx) + '\n'
		data[NETMON] = linesToWrite

	def updateNodeUsage(self, data):

		try:
			mdata = {}
			lines = os.popen("top -b -n 1 | tail -n +8 | awk '{ print $1 \" \" $2 \" \" $9 \" \" $10 \" \" $12}'").read().split('\n')
			for line in lines:
				if line != '':
					fields = line.strip().split(' ')
					if len(fields) == 5:
						try:
							pid = int(fields[0])
							user = utils.formatUserName(fields[1])
							if user not in mdata.keys():
								mdata[user] = []
							cpu = float(fields[2])
							mem = float(fields[3])
							command = fields[4]
							mdata[user].append((pid,command,cpu,mem))
						except:
							continue
			# Add to data the current time
			mdata['CURRENTTIME'] = utils.getCurrentTimeStamp()
		except:
			mdata = {}
		
		if lconfig.NODE_MONITOR_TYPE == 'db':
			data[USAGEMON] = json.dumps(mdata)
		else:
			data[USAGEMON] = mdata
		
	def updateNodeGPUUsage(self, data):
		
		try:
			mdata = {}
			lines = os.popen("nvidia-smi -q --display=UTILIZATION").read().split('\n')
			gpus = []
			memories = []
			for line in lines:
				if line != '':
					if line.count('Gpu') and line.count('%'):
						gpus.append(float(line.split(':')[-1].replace('%','')))
					elif line.count('Memory') and line.count('%'):
						memories.append(float(line.split(':')[-1].replace('%','')))
			for i in range(len(gpus)):
				mdata[i] = ((gpus[i],memories[i]))
			# Add to data the current time
			mdata['CURRENTTIME'] = utils.getCurrentTimeStamp()
		except:
			mdata = {}
		
		if lconfig.NODE_MONITOR_TYPE == 'db':
			data[GPUUSAGEMON] = json.dumps(mdata)
		else:
			data[GPUUSAGEMON] = mdata
			
	def run(self):
		hostName = utils.getHostName()
		print 'Initializing daemon...'
		sys.stdout.flush()
		if lconfig.NODE_MONITOR_TYPE == 'db':
			print 'NODE_MONITOR_TYPE: db'
			sys.stdout.flush()	
			userName = utils.getUserName()
			if userName not in lconfig.FULL_ACCESS_USERS:
				print 'Only users in ' + str(lconfig.FULL_ACCESS_USERS) + ' can run with db mode'
				sys.exit(2)
			print 'Getting DB connection (' + lconfig.NODE_MONITOR_DB_NAME + ',' +userName + ',' + lconfig.NODE_MONITOR_DB_HOST + ')...'
			sys.stdout.flush()
			connection = Connector(lconfig.NODE_MONITOR_DB_NAME, userName, lconfig.NODE_MONITOR_DB_HOST).getConnection()
			cursor = connection.cursor()
			cursor.execute('select ' + NAME + ' from ' + HOST + ' WHERE ' + NAME + ' = %s', (hostName,))
			if len(cursor.fetchall()) == 0:
				print 'ERROR: You still need to initialize the ' +  lconfig.NODE_MONITOR_DB_NAME + '. Use ExecuteLModule NMDaemonsManager -b'
				sys.exit(2)
			print 'Starting daemon loop...'
			sys.stdout.flush()
		counter = 0
		while True:
			data = {}
			self.updateNodeUsage(data)
			self.updateNetStatus(data)
			# We update the storage every 60 seconds
			if counter % 30 == 0:
				self.updateStorageStatus(data)
			# We update the GPU every 120 seconds
			#if counter % 60 == 0:
			#	self.updateNodeGPUUsage()
			
			if lconfig.NODE_MONITOR_TYPE == 'db':
				LEDDBOps.update(connection, HOST, data, {NAME:hostName,})
			else:
				for (key, method, usePickle) in [(USAGEMON,		getUsageFilePath,		True),
												 (NETMON,		getNetFilePath,			False),
												 (STORAGEMON,	getStorageFilePath,		False),
												 (GPUUSAGEMON,	getGPUUsageFilePath, 	True)]:
					kdata = data.get(key) 
					if kdata != None:
						if usePickle:
							f = open(method(hostName), "wb")
							pickle.dump(kdata, f)
						else:
							f = open(method(hostName), "w")
							f.write(kdata)
						f.close()				
			# We sleep 10 seconds until we do the next update
			time.sleep(10)
			# We increase counter for next iteration
			counter += 1
			
			if counter == 10000:
				counter = 0
			sys.stdout.flush()
			sys.stderr.flush()
			
if __name__ == "__main__":
	hostName = utils.getHostName()
	daemon = NodeMonitorDaemon(getPidFilePath(hostName), '/dev/null', getOutFilePath(hostName), getErrFilePath(hostName))
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			daemon.start()
		elif 'stop' == sys.argv[1]:
			daemon.stop()
		elif 'restart' == sys.argv[1]:
			daemon.restart()
		else:
			print "Unknown command"
			sys.exit(2)
		sys.exit(0)
	else:
		print "usage: %s start|stop|restart" % sys.argv[0]
		sys.exit(2)
