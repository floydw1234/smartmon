from pymongo import MongoClient
import pprint
import datetime
import numpy

client = MongoClient("mongodb://localhost:27017/pulses")
db = client.pulses
collRmsCur = db.RmsCurrent
collInstDem = db.InstDem
collVolt = db.Volt
collPwrFact = db.PwrFact
collOut = db.averages
collections = []
collections.append(collRmsCur)
collections.append(collInstDem)
collections.append(collVolt)
collections.append(collPwrFact)

#coll.insert_one(hourlyData)
#coll.insert_one(hourlyData1)
#array = coll.delete_many({"value": 1})
for i in range(0, len(collections)):
  totalValue = 0
  numValues = 0
  typeOfValue = ""
  for post in collections[i].find():
    totalValue= totalValue + post[u'value']
    numValues += 1


  if i == 0:
    typeOfValue = " Avg RmsCurrent"
  elif i ==1:
    typeOfValue = "Avg Instantaneous Demand"   
  elif i == 2:
    typeOfValue = "Avg Voltage"
  elif i == 3:
    typeOfValue = "Avg Power Factor"
    #insert more options if there are more data measurments added
  else:
    typeOfValue = "garbage"
    
  if numValues != 0: # just to make sure that we dont divide by zero. also checks that the collection has at least 1 document
    value = float(totalValue)/numValues
  else:
    print typeOfValue + " is an empty Collection"
    value = -2

  collOut.insert_one({"type" : typeOfValue, "value": value, "time" : datetime.datetime.utcnow()})
