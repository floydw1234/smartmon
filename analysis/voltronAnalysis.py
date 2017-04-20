from pymongo import MongoClient
import datetime

client = MongoClient("mongodb://localhost:27017/pulses")
db = client.pulses

# retrieve collections for all statistics the device spits out
currentSummationDelivered = db.CurrentSummationDelivered
instantaneousDemand = db.InstantaneousDemand
rmsCurrent = db.RmsCurrent
voltage = db.Voltage
powerFactor = db.PowerFactor

daily = db.Daily
averages = db.Averages

collections = []
collections.append(currentSummationDelivered)
collections.append(instantaneousDemand)
collections.append(rmsCurrent)
collections.append(voltage)
collections.append(powerFactor)

now = datetime.datetime.utcnow()
today = datetime.datetime(year=now.year, month=now.month, day=now.day, hour=7) # adjusting for time zone
if now.hour < 7:
	today = today - datetime.timedelta(days=1)

# "means" starts with the most recent hour, "deviceOn" starts with the most recent day
means = [[], [], [], [], [], [], [], [], [], [], [], []]
dailyInfo = [[]]
for i,collection in enumerate(collections):
	# calculate on/off time by using InstantaneousDemand collection
	if i == 1:
		for day in range(1):
			beginning = today - datetime.timedelta(days=day)
			end = beginning + datetime.timedelta(days=1)
			ascQuery = collection.find({"when": {"$gte": beginning, "$lt": end}}).sort("when", 1)

			mode = 0 # 0=off, 1=sleep, 2=on (low load), 3=on (high load)
			modeTime = [datetime.timedelta(), datetime.timedelta(), datetime.timedelta(), datetime.timedelta()]
			timeModeStarted = None
			last3Samples = []

			initial = True
			on = False

			turnedOn = 0
			timeTurnedOn = None

			timeOn = datetime.timedelta()
			timeOff = None

			endTime = None
			for document in ascQuery:
				demand = document["value"]
				time = document["when"]

				if initial:
					timeModeStarted = time
					if demand > 1: # instantaneousDemand threshold for being on/off
						on = True
						timeTurnedOn = time
						mode = 2
						if demand < 3: # instantaneousDemand threshold for being off/sleep
							mode = 1

					initial = False

				if len(last3Samples) != 3:
					last3Samples.append(demand)
				else:
					last3Samples = last3Samples[1:]
					last3Samples.append(demand)

				currentOn = False
				currentMode = 0
				if demand > 1:
					currentOn = True
					currentMode = 2
					if demand < 3:
						currentMode = 1
					elif demand >= 50 and len(last3Samples) == 3:
						if last3Samples[0] >= 50 and last3Samples[1] >= 50:
							currentMode = 3

				if not on and currentOn:
					turnedOn += 1
					on = True
					timeTurnedOn = time
				elif on and not currentOn:
					on = False
					timeOn += time - timeTurnedOn

				if mode == 0 and currentMode != 0:
					mode = currentMode
					modeTime[0] += time - timeModeStarted
					timeModeStarted = time
				elif mode == 1 and currentMode != 1:
					mode = currentMode
					modeTime[1] += time - timeModeStarted
					timeModeStarted = time
				elif mode == 2 and currentMode != 2:
					mode = currentMode
					modeTime[2] += time - timeModeStarted
					timeModeStarted = time
				elif mode == 3 and currentMode != 3:
					mode = currentMode
					modeTime[3] += time - timeModeStarted
					timeModeStarted = time

			if not initial:
				descQuery = collection.find({"when": {"$gte": beginning, "$lt": end}}).sort("when", -1)
				endTime = descQuery.next()["when"]
				modeTime[mode] += endTime - timeModeStarted
				if on:
					timeOn += endTime - timeTurnedOn

			timeAccountedFor = modeTime[0] + modeTime[1] + modeTime[2] + modeTime[3]

			if day == 0:
				timeOff = (now - beginning) - timeOn

				timeLeftover = (now - beginning) - timeAccountedFor
				modeTime[0] += timeLeftover
			else:
				timeOff = datetime.timedelta(days=1) - timeOn

				timeLeftover = datetime.timedelta(days=1) - timeAccountedFor
				modeTime[0] += timeLeftover

			date = beginning - datetime.timedelta(hours=7)

			dailyInfo[day] = [date, turnedOn, timeOn, timeOff, modeTime[1], modeTime[2], modeTime[3], beginning.weekday()]

	# calculate the average of each statistic for each of the past 12 hours
	for hour in range(12):
		total = 0
		upper = now - datetime.timedelta(hours=hour)
		lower = upper - datetime.timedelta(hours=1)
		query = collection.aggregate([{"$match": {"when": {"$gt": lower, "$lte": upper}}},
					      {"$group": {"_id": "null", "avg": {"$avg": "$value"}}}])

		success = False
		for doc in query:
			means[hour].append(doc["avg"]) # I believe this returns null if there are no values during this interval
			success = True

		if not success:
			means[hour].append(0)

	twelveHoursAgo = now - datetime.timedelta(hours=12)
	expirationDate = today
	if twelveHoursAgo < today:
		expirationDate = twelveHoursAgo
	collection.remove({"when": {"$lt": expirationDate}})

# Averages collection ends with the most recent hour
for i,mean in enumerate(reversed(means)):
	averages.remove({"hoursAgo": 11-i})
	averages.insert_one({"currentSummationDelivered": mean[0],
			     "instantaneousDemand": mean[1],
			     "rmsCurrent": mean[2],
			     "voltage": mean[3],
			     "powerFactor": mean[4],
			     "hoursAgo": 11-i})

for i,info in enumerate(reversed(dailyInfo)):
	if daily.count({"date": info[0]}) == 0 and daily.count() != 0:
		daysAgo = 1
		previousDay = info[0] - datetime.timedelta(days=1)
		while daily.count({"date": previousDay}) == 0:
			daysAgo += 1
			previousDay = previousDay - datetime.timedelta(days=1)
		for document in daily.find({"date" : previousDay}):
			lastWeekday = document["weekday"]
			timeOn = document["timeOn"]
			timeOff = document["timeOff"]
			if timeOn + timeOff != 24:
				daily.update({"date": previousDay}, {"$inc": {"timeOff": 24 - timeOn - timeOff}})
		nextDay = previousDay + datetime.timedelta(days=1)
		while daysAgo != 1:
			lastWeekday = (lastWeekday + 1) % 7
			daily.insert_one({"date": nextDay,
					  "turnedOn": 0,
					  "timeOn": 0,
					  "timeOff": 24,
					  "timeSleep": 0,
					  "timeLowLoad": 0,
					  "timeHighLoad": 0,
					  "weekday": lastWeekday})
			daysAgo -= 1
			nextDay = nextDay + datetime.timedelta(days=1)

	daily.remove({"date": info[0]})
	daily.insert_one({"date": info[0],
			  "turnedOn": info[1],
			  "timeOn": info[2].total_seconds() / 60 / 60,
			  "timeOff": info[3].total_seconds() / 60 / 60,
			  "timeSleep": info[4].total_seconds() / 60 / 60,
			  "timeLowLoad": info[5].total_seconds() / 60 / 60,
			  "timeHighLoad": info[6].total_seconds() / 60 / 60,
			  "weekday": info[7]})
