"""
Copyright 2017 Joe Talerico

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import yaml
import pprint
import datetime
import re
from Elastic import *

INDEX_DATA=True

es = Elastic('')

schedule = None
qdata = {}

RH_STORAGE = r"6048|720"
storage_nodes = []
general_nodes = []

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
                for host in value[date]['hosts']:
                    if re.match(RH_STORAGE,host) :
                       storage_nodes.append(host)
                    else:
                        general_nodes.append(host)
                obj['date'] = datetime.datetime.strptime(date,'%Y-%m-%d %H:%M:%S').isoformat()
                obj['cc'] = value[date]['cc']
                obj['owner'] = value[date]['owner']
                obj['project'] = value[date]['project']
                obj['hosts'] = value[date]['hosts']
                obj['ticket'] = value[date]['ticket']
                obj['num_hosts'] = len(value[date]['hosts'])
                obj['start_time'] = datetime.datetime.strptime(value[date]['start'],'%Y-%m-%d %H:%M:%S').isoformat()
                obj['end_time'] = datetime.datetime.strptime(value[date]['end'],'%Y-%m-%d %H:%M:%S').isoformat()

                if len(obj['hosts']) > 0 :
                    es.index(obj,'quads-history','history')
        obj = None
else:
    pprint.pprint(qdata)
