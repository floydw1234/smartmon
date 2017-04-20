#this will be for analysis script

import datetime
import json
import time
import numpy as np #this allows us to use np.median() for finding median of the list of values

def get_db(): 
	from pymongo import MongoClient
	client = MongoClient('localhost:27017/pulses3')
	db = client.test_mqtt
	return db
#prepare a cursor objext using cursor()method

# Open database connection/or create it on a virgin system


#try:
#		db.database = mysql_db
	#except:
	#	create_database(cursor)
	#	db.database = mysql_db
	#	print created database


	#if checkTableExists(db, data):
	#	cursor.excecute("""CREATE TABLE `SmartMon`.`data` ( `SENSOR` TEXT NOT NULL , `VALUE` FLOAT(8) NOT NULL , `DATETIME` INT(11) NOT NULL ) ENGINE = InnoDB;""")
	
	#"SELECT SENSOR, VALUE, DATETIME FROM data where (SENSOR = (%s) And VALUE >= %s )", (str(Sensor[0]),int(startTime-3600))) #Cast(VALUE AS BIGINT)

timeStart = datetime.datetime.now()
def checkTableExists(cursor, tablename):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        cursor.close()
        return True

    cursor.close()
    return False

def truncate(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])

		
def create_table(cursor):
    try:
		cursor.execute("""CREATE TABLE `hourlyData` ( `SENSOR` TEXT NOT NULL , `MEDIAN` FLOAT(8) NOT NULL, `MEAN` FLOAT(8) NOT NULL , LASTDATETIME DATETIME NOT NULL ) ENGINE = InnoDB;""", NULL)
    except:
        print "could not create table"
		

def createOrCheckTableAnalysis(cursor, tablename):
	if checkTableExists(cursor, tablename) == False:
		create_table(cursor)
	else:
		print "Table Found!"

	cursor.close()
def createSensorList(cursor):
	cursor.execute("SELECT SENSOR FROM data")
	allSensors = cursor.fetchall()
	uniqueSensorList = list(set(allSensors))
	for Sensor in uniqueSensorList:
		
		print Sensor[0]
	return uniqueSensorList

def compileAndWriteData(cursor, uniqueSensorList):
	for Sensor in uniqueSensorList:
		query = "".join(("SELECT SENSOR, VALUE, DATETIME FROM data WHERE (SENSOR = '", str(Sensor[0]),"'  AND DATETIME >= DATE_SUB(now(), INTERVAL '24:0' HOUR_MINUTE))")) # '1:0' = 	1 hour and 0 minutes
		cursor.execute(query)
		list = cursor.fetchall() #fetching all rows with matching criteria from above
		allNumbers = []           # initializing the array that will be used to find median
		for row in list:
			allNumbers.append(row[1]) #row[1] is the value

		median = np.median(allNumbers)
		mean = np.mean(allNumbers)
		list_result = []
		list_result.append(median)
		list_result.append(mean)
		list_result[0] = truncate(list_result[0],7)
		list_result[1] = truncate(list_result[1],7)
		print "median =" 
		print list_result[0]
		print "mean ="
		print list_result[1]
		
		createOrCheckTableAnalysis(cursor, table_name)
		cursor = db.cursor()
		try:
			cursor.execute("""INSERT INTO `hourlyData` (`SENSOR`, `MEDIAN`, `MEAN`, `DATETIME`) VALUES (%s, %s, %s, %s)""", (str(Sensor[0]),str(list_result[0]),str(list_result[1]),timeStart))
			db.commit();
			print "wrote into hourlyData"
		except:
			try:
				db.rollback()
				cursor.execute("""INSERT INTO `hourlyData` (`SENSOR`, `MEDIAN`, `MEAN`, `DATETIME`) VALUES (%s, %s, %s, %s)""", (str(Sensor[0]),str(-1),str(-1),timeStart)) #puts in -1 for if no data is found for last hour
				db.commit()
				print "no data in last hour. Inserting -1 into mean and median"
			except:
				db.rollback()

		
#datetime.datetime.fromDATETIME(




#PROGRAM STARTS HERE!!!!!!!!!



#query = "CREATE TABLE `SmartMon`.`hourlyData` ( `SENSOR` TEXT NOT NULL , `VALUE` FLOAT(8) NOT NULL , `DATETIME` DATETIME NOT NULL ) ENGINE = InnoDB"
#print query
try:
	db = MySQLdb.connect(mysql_server, mysql_username, mysql_passwd, mysql_db)
	cursor = db.cursor()
except:
	print "failed to connect to" + mysql_db 


uniqueSensorList  = createSensorList(cursor)
compileAndWriteData(cursor, uniqueSensorList)


'''
try:
    cursor.execute("""INSERT INTO data VALUES (%s,%s,%s)""", (str("Sensor Name"),str(msg.payload),str(int(time.time())))) #sends value as unix-gmt
    db.commit()
except:
	db.rollback()
	print "could not write to data, VALUES\n"
	checkorCreateTable(db, cursor);
'''
