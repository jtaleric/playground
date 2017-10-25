import yaml
import pprint
import datetime
from Elastic import *

INDEX_DATA=True

es = Elastic('')

schedule = None
qdata = {}

with open('schedule.yaml', 'r') as schedule_file :
    try :
        schedule = yaml.load(schedule_file)
    except:
        print "ERROR"

if schedule is None :
    print "ERROR"

for cloud,data in schedule['cloud_history'].iteritems() :
    previous_ts = None
    if cloud not in qdata :
        qdata[cloud] = {}
    if type(data) is dict :
        for date in sorted(data) :
            try :
                int(date)
            except :
                continue
            ts = datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
            if previous_ts is not None:
                qdata[cloud][previous_ts]['next_ts'] = ts
                previous_ts = None
            if ts not in qdata[cloud] :
                qdata[cloud][ts] = {}
                qdata[cloud][ts]['hosts'] = []
                qdata[cloud][ts]['start'] = None
                qdata[cloud][ts]['end'] = None
                qdata[cloud][ts]['ticket'] = data[date]['ticket']
                qdata[cloud][ts]['owner'] = data[date]['owner']
                qdata[cloud][ts]['cc'] = data[date]['ccusers']
                qdata[cloud][ts]['project'] = data[date]['description']

                previous_ts = ts

for host,data in schedule['hosts'].iteritems() :
    if type(data) is dict:
        for pos in data['schedule'] :
            if data['schedule'][pos]['cloud'] in qdata :
                for date in qdata[data['schedule'][pos]['cloud']] :
                    if 'next_ts' in qdata[data['schedule'][pos]['cloud']][date] :
                        if data['schedule'][pos]['start'] > date < data['schedule'][pos]['end'] and data['schedule'][pos]['start'] < qdata[data['schedule'][pos]['cloud']][date]['next_ts'] > data['schedule'][pos]['end'] :
                                qdata[data['schedule'][pos]['cloud']][date]['hosts'].append(host)
                                qdata[data['schedule'][pos]['cloud']][date]['start'] = data['schedule'][pos]['start']
                                qdata[data['schedule'][pos]['cloud']][date]['end'] = data['schedule'][pos]['end']
                    elif data['schedule'][pos]['start'] > date < data['schedule'][pos]['end'] :
                        qdata[data['schedule'][pos]['cloud']][date]['hosts'].append(host)
                        qdata[data['schedule'][pos]['cloud']][date]['start'] = data['schedule'][pos]['start']
                        qdata[data['schedule'][pos]['cloud']][date]['end'] = data['schedule'][pos]['end']

if INDEX_DATA :
    for key,value in qdata.iteritems():
        obj = { 'cloud': key }
        if type(value) is dict :
            for date in value :
                obj['date'] = datetime.datetime.strptime(date,'%Y-%m-%d %H:%M:%S').isoformat()
                obj['cc'] = value[date]['cc']
                obj['owner'] = value[date]['owner']
                obj['project'] = value[date]['project']
                obj['hosts'] = value[date]['hosts']
                obj['ticket'] = value[date]['ticket']
                obj['num_hosts'] = len(value[date]['hosts'])
                obj['start_time'] = value[date]['start']
                obj['end_time'] = value[date]['end']

                if len(obj['hosts']) > 0 :
                    es.index(obj,'quads-history','history')
        obj = None
else:
    pprint.pprint(qdata)
