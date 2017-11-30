#!/usr/bin/env python

# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import gzip
import logging
import argparse
import datetime
from google.cloud import pubsub_v1
import os

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TOPIC = 'sandiego'
INPUT = 'sensor_obs2008.csv.gz'
PROJECT_ID = os.getenv('DEVSHELL_PROJECT_ID')

#def publish(topic, events):
#   numobs = len(events)
#   if numobs > 0:
#      with topic.batch() as batch:
#         logging.info('Publishing {} events from {}'.
#                    format(numobs, get_timestamp(events[0])))
#         for event_data in events:
#              batch.publish(event_data)
              
def publish(topic_path, events):
   publisher = pubsub_v1.PublisherClient()
   numobs = len(events)
   if numobs > 0:
      logging.info('Publishing {} events from {}'.
      	  format(numobs, get_timestamp(events[0])))
      for event_data in events:
          publisher.publish(topic_path, data=event_data.encode('utf-8'))
####              
#    publisher = pubsub_v1.PublisherClient()
#    topic_path = publisher.topic_path(project, topic_name)
#
#    for n in range(1, 10):
#        data = u'Message number {}'.format(n)
#        # Data must be a bytestring
#        data = data.encode('utf-8')
#        publisher.publish(topic_path, data=data)

def get_timestamp(line):
   # look at first field of row
   timestamp = line.split(',')[0]
   return datetime.datetime.strptime(timestamp, TIME_FORMAT)

#def simulate(topic, ifp, firstObsTime, programStart, speedFactor):
def simulate(topic_path, ifp, firstObsTime, programStart, speedFactor):
   # sleep computation
   def compute_sleep_secs(obs_time):
        time_elapsed = (datetime.datetime.utcnow() - programStart).seconds
        sim_time_elapsed = (obs_time - firstObsTime).seconds / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs

   topublish = list() 

   for line in ifp:
       event_data = line   # entire line of input CSV is the message
       obs_time = get_timestamp(line) # from first column

       # how much time should we sleep?
       if compute_sleep_secs(obs_time) > 1:
          # notify the accumulated topublish
          publish(topic_path, topublish) # notify accumulated messages
          topublish = list() # empty out list

          # recompute sleep, since notification takes a while
          to_sleep_secs = compute_sleep_secs(obs_time)
          if to_sleep_secs > 0:
             logging.info('Sleeping {} seconds'.format(to_sleep_secs))
             time.sleep(to_sleep_secs)
       topublish.append(event_data)

   # left-over records; notify again
   publish(topic_path, topublish)

def peek_timestamp(ifp):
   # peek ahead to next line, get timestamp and go back
   pos = ifp.tell()
   line = ifp.readline()
   ifp.seek(pos)
   return get_timestamp(line)


if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Send sensor data to Cloud Pub/Sub in small groups, simulating real-time behavior')
   parser.add_argument('--speedFactor', default=60, help='Example: 60 implies 1 hour of data sent to Cloud Pub/Sub in 1 minute', required=False, type=float)
   args = parser.parse_args()

   # create Pub/Sub notification topic
   logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
   
   publisher = pubsub_v1.PublisherClient()
   topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
   try :
       topic = publisher.create_topic(topic_path)
       logging.info('Creating pub/sub topic {}'.format(TOPIC))
   except Exception:
       pass

   # notify about each line in the input file
   programStartTime = datetime.datetime.utcnow() 
   with gzip.open(INPUT, 'rb') as ifp:
      header = ifp.readline()  # skip header
      firstObsTime = peek_timestamp(ifp)
      logging.info('Sending sensor data from {}'.format(firstObsTime))
      simulate(topic_path, ifp, firstObsTime, programStartTime, args.speedFactor)
