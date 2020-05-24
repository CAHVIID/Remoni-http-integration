############################################################################
#
# HTTP to Azure Event hub bridge
#
# 09-05-2020 2.0 version    Christian Anker Hviid cah@byg.dtu.dk
# Inspired by ATEA script by Bjarke Bolding Rasmussen BBRA@atea.dk
#
# Integration bridge from Remoni cloud to Azure Event hub 
#
# Help sources
# https://api.remoni.com/v1#
#
############################################################################

scriptversion = 1.0

import requests, time, json
import schedule
from datetime import datetime
from geopy import geocoders
from azure.eventhub import EventHubClient, EventData
from azure.cosmos import cosmos_client

cfg = json.load(open('config.json'))

############ Load token from tmp file

def loadToken():
    token = {}
    try:
        token = json.load(open('session.tmp'))
        token
        return token
    except Exception as e:
        raise e

############ Get units

def getUnits(token):
    baseURL = cfg['baseURL']
    accountId = cfg['accountId']
    URL = f'{baseURL}/Units?orderby=UnitId&Account.AccountId=eq({accountId})&top=10000'
    r = requests.get(URL, headers={"authorization":token,"content-type":"application/json"})
    return r.json()

############ Get timezone of unit's location, not used currently

def getUnitTimeZone(lat,lon):
    g = geocoders.GoogleV3()
    tz = str(g.timezone((lat, lon)))
    #time = timezone(tz) #local time in tz
    return tz

############ Get timestamp of latest entry in database

def getStartTimestamp(unit, clientdb, CONTAINER_LINK, FEEDOPTIONS):
    try:
        QUERY = {
                "query": f"SELECT TOP 1 c.ts from c WHERE CONTAINS(c.deviceid, '{unit}')"
                #"query": f"SELECT TOP 1 * from c"
        }

        results = clientdb.QueryItems(CONTAINER_LINK, QUERY, FEEDOPTIONS)
        jdata = json.dumps(list(results))
        start = json.loads(jdata)[0]['ts']
    except:
        start = None
    return start

############ Logprint

def logPrint(log):
    print(str(datetime.utcnow()) + ' UTC: ' + str(log))
     
############ Numeric validation function

def is_numeric(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return

############ Get missing values from units

def getMissingValues(units, token):
    # Internal, external#1 (return), external#2 (supply) 
    sensortypes = cfg['unitTypeInputId']
    data = []
    for unit in units:
        unitid = unit['UnitId']
        newUnit = {
            'unitid': unitid,
            'name': unit['Name'],
            'type': unit['UnitType']['Name'],
            'lat': unit['Latitude'],
            'lon': unit['Longitude'],
            'telemetry': []
        }

        # Create time interval for data extraction
        starttime = getStartTimestamp(unit, clientdb, CONTAINER_LINK, FEEDOPTIONS)
        starttime= round(time.time()*1000)-(60*60*6*1000) #import only 6 hours back, delete after testing!
        
        if starttime == None:
            from_time = 1587679200000 # Apr 24 2020 UTC, ms, #1483228800000 #2017-01-01T00:00:00Z, Jan 1 2017 UTC in milliseconds
        else:
            from_time = datetime.fromtimestamp(int(round(starttime/1000))).strftime('%Y-%m-%dT%H:%M:%SZ')
#            from_time = datetime.fromtimestamp(int(round(starttime))).strftime('%Y-%m-%dT%H:%M:%SZ') #for testing
        to_time = datetime.utcnow()

        # GET request for each sensortype in timeinterval
        for sensor in sensortypes:
#            parameters = f'Timestamp=gt({from_time})&UnitId=eq({unit})&AggregateType=eq(Minutes5)&DataType=eq(temperature)&UnitTypeInputId=eq({sensor})&top=10000'
            parameters = f'Timestamp=gt({from_time})&Timestamp=lt({to_time})&UnitId=eq({unitid})&AggregateType=eq(Hour)&DataType=eq(temperature)&UnitTypeInputId=eq({sensor})&top=10000'

            URL = cfg['baseURL'] + '/Data?orderby=Timestamp&' + parameters
            r = requests.get(URL, headers={"authorization":token,"content-type":"application/json"})
            if r.status_code == 200:
                newUnit['telemetry'].append(r.json())
            else:
                logPrint("HTTP request failed!")

        data.append(newUnit)

    return data

############ Send payload to Azure Event Hub function

def sendToEventHub(data):
    try:
        client = EventHubClient(
            cfg['EventHubURL'], debug=False, username=cfg['EventHubPolicyName'], password=cfg['EventHubPrimaryKey'])
        sender = client.add_sender(partition="0")
        client.run()
        try:
            count = 0
            for payload in data:
                sender.send(EventData(json.dumps(payload)))
                #logPrint("Payload sent: " + json.dumps(payload))
                count += 1
        except:
            logPrint('Send to Eventhub failed')
            raise
        finally:
            logPrint(str(count) + ' payloads sent')
            data.clear()
            client.stop()
    except KeyboardInterrupt:
        pass

############ Timer 

def timedDataTransfer():
    token = json.loads(loadToken())
    units = getUnits(token['access_token'])
    data = getMissingValues(units, token['access_token'])
    sendPackage(data)

############ Packaging payload for Eventhub

def sendPackage(data):
    allUnits = []
    
    for unit in data:
        telemetry = unit['telemetry']
                
        telemetryContentIsTrue = telemetry[0]
        if telemetryContentIsTrue: #Check if telemetry is not empty
            newUnit = {
                'deviceid' : unit['unitid'],
                'devicename' : unit['name'],
                'devicetype' : unit['type'],
                'attributes' : {
                        'location' : 'borgerskolen',
                        'latitude' : unit['lat'],
                        'longitude' : unit['lon'],
                        },
                'telemetry' : [],
            }

            tele={}
            try:
                internalSensor = telemetry[0]
                externalSensor1 = telemetry[1]
                externalSensor2 = telemetry[2]

                for i, measure in enumerate(internalSensor):
                    tele = {'ts' : round(datetime.timestamp(datetime.strptime(measure['Timestamp'],"%Y-%m-%dT%H:%M:%S%z"))*1000),
                            'values' : {}}
                    tele['values']['temperature_int'] = round(internalSensor[i]['Value'],2)
                    tele['values']['temperature_ext1'] = round(externalSensor1[i]['Value'],2)
                    tele['values']['temperature_ext2'] = round(externalSensor2[i]['Value'],2)
                    newUnit['telemetry'].append(tele)  
                
#                for eachSensorType in telemetry:
#                    for i, measure in enumerate(eachSensorType):
#                        tele[i] = {'ts' : round(datetime.timestamp(datetime.strptime(measure['Timestamp'],"%Y-%m-%dT%H:%M:%S%z"))*1000),
#                                'values' : {}}
#                        for key in measure.keys():
#                            if key == 'UnitTypeInputId' and measure[key] == 3101:
#                                tele[i]['values']['temperature_int'] = round(measure['Value'],2)
#                            elif key == 'UnitTypeInputId' and measure[key] == 3102:
#                                tele[i]['values']['temperature_ext1'] = round(measure['Value'],2)
#                            elif key == 'UnitTypeInputId' and measure[key] == 3103:
#                                tele[i]['values']['temperature_ext2'] = round(measure['Value'],2)
    #                else:
#                newUnit['telemetry'].append(tele)
            except Exception as e:
                logPrint('Telemetry processing failed for device: ' + str(newUnit['deviceid']) + ', ' + newUnit['devicename'] +'. Error: ' + str(e))
                print(telemetry)
        
            allUnits.append(newUnit)
    
    sendToEventHub(allUnits)

############ Main
#Init
data = []

DB_ENDPOINT = cfg["DB_ENDPOINT"]
DB_PRIMARYKEY = cfg["DB_PRIMARYKEY"]
DATABASE = cfg["DATABASE"]
CONTAINER = cfg["CONTAINER"]
CONTAINER = 'sandbox'
CONTAINER_LINK = f"dbs/{DATABASE}/colls/{CONTAINER}"
FEEDOPTIONS = {}
FEEDOPTIONS["enableCrossPartitionQuery"] = True

# Initialize the Cosmos client        
clientdb = cosmos_client.CosmosClient(
        url_connection = DB_ENDPOINT, auth={"masterKey": DB_PRIMARYKEY})

# header
logPrint('Azure EventHub: ' + cfg['EventHubURL'])
logPrint('Configured to send payloads every ' + cfg['EventHubBatchTimer'] + ' seconds')

# Scheduled auth (eventually tweak to read token expiration)
#schedule.every().day.do(auth)
# Scheduled data management - read data by GET to send to eventhub
timedDataTransfer()

schedule.every(int(cfg['EventHubBatchTimer'])).seconds.do(timedDataTransfer)
#schedule.every(10).minutes.do(timedDataTransfer)


while True:
    schedule.run_pending()
    time.sleep(int(cfg['EventHubBatchTimer']))
    time.sleep(1)