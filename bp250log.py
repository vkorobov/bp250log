#!/usr/bin/env python
#
# BusinessPhone 250 calls data logger to MySQL
#
# Copyright (c) 2015 Vitaly Korobov (vkorobov)
#
# Version 1.41
#

from ConfigParser import ConfigParser
from datetime import datetime, timedelta, time
import time
import mysql.connector
import xmpp,sys
from serial import Serial, SerialException, SerialTimeoutException
import serial

class EnhancedSerial(Serial):
	def __init__(self, *args, **kwargs):
		#ensure that a reasonable timeout is set
		timeout = kwargs.get('timeout',0.1)
		if timeout < 0.01: timeout = 0.1
		kwargs['timeout'] = timeout
		Serial.__init__(self, *args, **kwargs)
		self.buf = ''

	def readline(self, maxsize=None, timeout=1):
		"""maxsize is ignored, timeout in seconds is the max time that is way for a complete line"""
		tries = 0
		while 1:
			self.buf += self.read(512)
			pos = self.buf.find('\n')
			if pos >= 0:
				line, self.buf = self.buf[:pos+1], self.buf[pos+1:]
				return line
			tries += 1
			if tries * self.timeout > timeout:
				break
		line, self.buf = self.buf, ''
		return line

	def readlines(self, sizehint=None, timeout=1):
		"""read all lines that are available. abort after timout
		when no more data arrives."""
		lines = []
		while 1:
			line = self.readline(timeout=timeout)
			if line:
				lines.append(line)
			if not line or line[-1:] != '\n':
				break
		return lines


def xmpp_send_alarm(xmpp_msg):
	client.send(xmpp.protocol.Message(xmpp_conf,PBX_ID + ": " + xmpp_msg,'groupchat'))
#	for debug
	client.send(xmpp.protocol.Message('vk@spd.tlt.ru',PBX_ID + ": " + xmpp_msg))
	errfile = open("bp250log_err.log", "a")
	errfile.write(str(datetime.now())+' '+str(xmpp_msg)+'\r\n')
	errfile.close()


def read_db_config(filename='bp250log.ini', section='mysql'):
	""" Read database configuration file and return a dictionary object
	:param filename: name of the configuration file
	:param section: section of database configuration
	:return: a dictionary of database parameters
	"""
	# create parser and read ini configuration file
	parser = ConfigParser()
	parser.read(filename)

	# get section, default to mysql
	db = {}
	if parser.has_section(section):
		items = parser.items(section)
		for item in items:
			db[item[0]] = item[1]
	else:
		raise Exception('{0} not found in the {1} file'.format(section, filename))

	return db

def read_serial_config(filename='bp250log.ini', section='serial'):
	""" Read database configuration file and return a dictionary object
	:param filename: name of the configuration file
	:param section: section of database configuration
	:return: a dictionary of database parameters
	"""
	# create parser and read ini configuration file
	parser = ConfigParser()
	parser.read(filename)

	# get section, default to mysql
	serp = {}
	if parser.has_section(section):
		items = parser.items(section)
		for item in items:
			serp[item[0]] = item[1]
	else:
		raise Exception('{0} not found in the {1} file'.format(section, filename))
	return serp

def read_xmpp_config(filename='bp250log.ini', section='xmpp'):
	""" Read database configuration file and return a dictionary object
	:param filename: name of the configuration file
	:param section: section of database configuration
	:return: a dictionary of database parameters
	"""
	# create parser and read ini configuration file
	parser = ConfigParser()
	parser.read(filename)

	# get section, default to mysql
	serp = {}
	if parser.has_section(section):
		items = parser.items(section)
		for item in items:
			serp[item[0]] = item[1]
	else:
		raise Exception('{0} not found in the {1} file'.format(section, filename))
	return serp


def connect():
	# """ Connect to MySQL database """

	db_config = read_db_config()

	try:
		#print('Connecting to MySQL database...')
		#xmpp_send_alarm('Connecting to MySQL database...')
		conn = mysql.connector.connect(**db_config)

		if conn.is_connected():
			print('Database connection established.')
			xmpp_send_alarm('Database connection established.')
		else:
			print('connection failed.')
			xmpp_send_alarm('Database connection failed.')

	except mysql.connector.Error as err:
		print("Connection failed: ", format(err))
		xmpp_send_alarm("Stopped. Connecting to database failed: " + str(err))
		exit()
	else:
		return conn
	finally:
		pass

def write2db(conn, query, args):
	""" Write data to MySQL database """
	try:
		cursor = conn.cursor()
		cursor.execute(query,args)
	except mysql.connector.Error as err:
		print("Error insert to database: ", err)
		xmpp_send_alarm("Error. Insert into database failed: " + format(err))
		errfile = open("bp250log_err.log", "a")
		errfile.write(str(datetime.now())+' '+str(args)+'\r\n')
		errfile.close()
		exit()
	finally:
		conn.commit()
		cursor.close()

if __name__=='__main__':
	#CIL="150410 09:21:35 127      51                       08:06:17 0000 #430 351                      26775                         c   3    26 "
	#CIL="150410 09:21:59 #402     679                      00:00:20 0000                               45599                      12 A        2B "
	#CIL="150410 09:22:05 #401     149                      00:00:39 0001                               45595 8482797613              A C      20 "
	#150410 09:22:24 #402     102                               0025                               45599                      11 A        2B
	#CIL="150 410 09:22:15 102      148                               0016                               45601                         Q        B0 "
	#150410 09:22:57 198      9700946                  00:00:58 0012 #403 9700946                  45565                         a   9    06
	#150410 09:23:13 #455     125                      00:01:03 0003                               45603 8482702060              A        20
	#CIL="150410 09:23:14 650      989277720721             00:00:46 0024 #404 989277720721             45567                         a   9    06 "


	# calculate UTC offset
	dt = datetime.now() # datetime in localzine
	ts = time.mktime(dt.timetuple()) #  timestamp localzone
	utc_offset = datetime.fromtimestamp(ts) - datetime.utcfromtimestamp(ts)


	# jabber init
	jabber = read_xmpp_config()

	xmpp_jid = jabber['jid']
	xmpp_pwd = jabber['pwd']
	xmpp_conf = jabber['conf']
	xmpp_nick = jabber['nick']

	jid = xmpp.protocol.JID(xmpp_jid)
	client = xmpp.Client(jid.getDomain(),debug=[])
	client.connect()
	client.auth(jid.getNode(),str(xmpp_pwd),resource='AlarmTerminal')

	client.sendInitPresence(1)
	client.send(xmpp.Presence(to="%s/%s" % (xmpp_conf, xmpp_nick)))

	# init serial port
	serc = read_serial_config()
	PBX_ID=serc['pbx_id']

	xmpp_send_alarm("Started...")

	# connect to db
	conn=connect()


	try:
#		ser = EnhancedSerial(
		ser = serial.Serial(
			port = serc['port'],
			baudrate = int(serc['baudrate']),
			bytesize = int(serc['bytesize']),
			parity = serc['parity'],
			stopbits = int(serc['stopbits']),
			timeout = int(serc['timeout']),
			xonxoff = int(serc['xonxoff']),
			rtscts = int(serc['rtscts'])
		)
		print (ser)
		print("Serial port is open")
	except SerialTimeoutException as err:
		print ("Serial port timeout: " + str(err))
		xmpp_send_alarm("Serial port timeout: " + str(err))
	except IOError as err:
		print ("Error open serial port: " + str(err))
		xmpp_send_alarm("Stopped... Error open serial port: " + str(err))
		exit()


	# read data
	if ser.isOpen():
		xmpp_send_alarm("Open port...")

		while True:
			try:
				CIL = ser.readline()
			except SerialException as err:
				print ("error communicating...: " + str(err))
				xmpp_send_alarm("Error communicating: " + str(err))
			except (KeyboardInterrupt, SystemExit):
				print '\nProgram Stopped Manually!'
				xmpp_send_alarm("Stopped Manually...")
				ser.close()
				conn.close()
				client.disconnect()
				raise
			else:
				if not CIL:
					# print (str(datetime.now())+' Timeout: no data from serial port...')
					sys.stdout.write('.')
					pass
				else:
					print ('\n'+CIL),
					try:
						# parse
						CallEndTime=datetime.strptime(CIL[0:15], "%y%m%d %H:%M:%S")
						EXT=CIL[16:24]
						DialledNumber=CIL[25:49]
						CallDuration=CIL[50:58]
						QueueTime=datetime.time(datetime.strptime("00:"+CIL[59:61]+":"+CIL[61:63], "%H:%M:%S"))
						Trunk=CIL[64:68]
						SentNumber=CIL[69:93]
						TAG=int(CIL[94:99])
						Anumber=CIL[100:120]
						Transfer=CIL[121:123]
						CallFacil=CIL[128:129]
						Status=CIL[124:125]
						CallViaFacil=CIL[126:127]
						ACC=CIL[128:132]
						ORGTERM=CIL[133:135]

						if CallDuration == '        ':
							CallDuration=datetime.time(datetime.strptime("00:00:00", "%H:%M:%S"))
						else:
							CallDuration=datetime.time(datetime.strptime(CallDuration, "%H:%M:%S"))

						CallStartTime=CallEndTime-timedelta(hours=CallDuration.hour,minutes=CallDuration.minute,seconds=CallDuration.second)-timedelta(hours=0,minutes=QueueTime.minute,seconds=QueueTime.second)
						CallStartTimeUTC=CallStartTime-utc_offset
						DurationSec=int(CallDuration.hour*3600+CallDuration.minute*60+CallDuration.second)

						"""
						print ("Call Start Date UTC:\t", CallStartTimeUTC)
						print ("Call Start Date:\t", CallStartTime)
						print ("Call End Date:\t", CallEndTime)
						print ("Extension:\t", EXT)
						print ("Access code:\t", ACC)
						print ("Dialled:\t", DialledNumber)
						print ("Duration:\t", CallDuration)
						print ("Duration,s:\t", DurationSec)
						print ("Queue Time:\t", QueueTime)
						print ("Trunk Number:\t", Trunk)
						print ("Sent Number:\t", SentNumber)
						print ("TAG Number:\t", TAG)
						print ("A-Number:\t", Anumber)
						print ("Transfer Condition:\t",Transfer)
						print ("Call via Facility:\t", CallViaFacil)
						print ("Status:\t", Status)
						print ("ORG/TERM:\t", ORGTERM)
						"""
					except Exception as err:
						print ("Warning: Parse error", err)
						xmpp_send_alarm("Warning: Parse error: " + str(err))
					else:
						try:
							# export to db
							query = "insert into CALLS (CALL_TIME_START_UTC, CALL_DATE_START, CALL_TIME_START, CALL_DATE_END, CALL_TIME_END, TAG, TRUNK, EXT, DURATION, DURATION_S, RING, A_NUMBER, ACC, DIALLED_NUMBER, SENT_NUMBER, ORG_TERM, TRANSFER_CONDITION, CALL_TYPE, CALL_VIA_FAC, PBX_IDENTIFIER)" \
									"values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
							args =  (CallStartTimeUTC, CallStartTime.date(), CallStartTime.time(), CallEndTime.date(), CallEndTime.time(), TAG, Trunk.strip(), EXT.strip(), CallDuration, DurationSec, QueueTime, Anumber.strip(), ACC.strip(), DialledNumber.strip(), SentNumber.strip(), ORGTERM, Transfer.strip(), Status, CallViaFacil.strip(), PBX_ID )

							# print(args)

							write2db(conn, query, args)
						except Exception as err:
							print ("Error: Database problem", err)
							xmpp_send_alarm("Stopped: Error " + str(err))
							exit()

	else:
		print ("cannot open serial port ")
		xmpp_send_alarm("Stopped: cannot open serial port")
		exit()

	xmpp_send_alarm("Stopped...")
	ser.close()
	conn.close()
	client.disconnect()
