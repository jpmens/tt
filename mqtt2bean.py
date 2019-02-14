#!/usr/bin/env python -B
# -*- coding: utf-8 -*-

import paho.mqtt.client as paho   # pip install paho-mqtt
import ssl
import csv
import time
import socket
import sys
import os
import beanstalkc
import json
import errno
import time
import uuid
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import configparser

__author__    = 'Jan-Piet Mens <jp@mens.de>'
__copyright__ = 'Copyright 2017-2019 Jan-Piet Mens'

def setup_log(filename):
    logger = logging.getLogger('mqtt2bean')
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.DEBUG)

    if filename:
        handler = RotatingFileHandler(
            filename=filename, maxBytes=1*1024*1024, backupCount=5)
    else:
        handler = logging.StreamHandler()

    fmt = logging.Formatter('%(asctime)s:%(levelname)s:%(filename)s:%(message)s')
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger

#   _        _           _   
#  (_)_ __  (_) ___  ___| |_ 
#  | | '_ \ | |/ _ \/ __| __|
#  | | | | || |  __/ (__| |_ 
#  |_|_| |_|/ |\___|\___|\__|
#         |__/               

cf = configparser.ConfigParser()
cf.read(os.getenv('MQTT2BEAN', 'mqtt2bean.conf'))
logfile_name = cf.get("defaults", "logfile")
topics = cf.get("defaults", "topics")
host = cf.get("defaults", "host")
port = int(cf.get("defaults", "port"))
cafile = cf.get("defaults", "cafile")
username = cf.get("defaults", "username")
password = cf.get("defaults", "password")

topics = topics.split()

_logger = setup_log(logfile_name)

#                   _   _   ____  _                      
#   _ __ ___   __ _| |_| |_|___ \| |__   ___  __ _ _ __  
#  | '_ ` _ \ / _` | __| __| __) | '_ \ / _ \/ _` | '_ \ 
#  | | | | | | (_| | |_| |_ / __/| |_) |  __/ (_| | | | |
#  |_| |_| |_|\__, |\__|\__|_____|_.__/ \___|\__,_|_| |_|
#                |_|                                     

TUBE = 'totraccar'

if sys.version < '3':
    import codecs
    def u(x):
        return codecs.unicode_escape_decode(x)[0]
else:
    def u(x):
        return x

def payload2location(topic, payload):

    item = {}

    # Payloads are either JSON or CSP.
    try:
        item = json.loads(payload)
        if type(item) != dict:
            return None
    except ValueError:
        # eg: "K2,542A46AA,k,40365854,4575769,26,4,7,5,8"
        MILL = 1000000.0

        fieldnames = ['tid', 'tst', 't', 'lat', 'lon', 'cog', 'vel', 'alt', 'dist', 'trip' ]

        try:
            csvreader = csv.DictReader(io.StringIO(u(payload)), fieldnames=fieldnames)
            for r in csvreader:
                item = {
                    '_type' : 'location',
                    'tid'   : r.get('tid', '??'),
                    'tst'   : int(r.get('tst', 0), 16),
                    't'     : r.get('t', 'X'),
                    'lat'   : float(float(r.get('lat')) / MILL),
                    'lon'   : float(float(r.get('lon')) / MILL),
                    'cog'   : int(r.get('cog', 0)) * 10,
                    'vel'   : int(r.get('vel', 0)),
                    'alt'   : int(r.get('alt', 0)) * 10,
                    'dist'  : int(r.get('dist', 0)),
                    'trip'  : int(r.get('trip', 0)) * 1000,
                }
                # print (json.dumps(item, sort_keys=True))
        except Exception as e:
            _logger.error("CSV decoding fails for {0}: {1}".format(topic, str(e)))
            return None
    except:
        _logger.error("Payload decoding fails for {0}: {1}".format(topic, str(e)))
        return None

    if 'tid' not in item:
        if topic is None:
            item['tid'] = 'zZ'
        else:
            item['tid'] = topic[-2:]
    if 't' not in item:
        item['t'] = '-'

    if topic is None:
        topic = 'owntracks/_/' + item['tid']

    # Coerce numeric to types. Older JSON description allowed strings for these.

    for elem in ['tst', 'cog', 'vel', 'alt', 'dist', 'trip']:
        item[elem] = int(item.get(elem, 0))
    for elem in ['lat', 'lon']:
        try:
            item[elem] = float(item.get(elem))
        except:
            pass


    # Sanity checks

    if '_type' not in item:
        item['_type'] = 'location'

    if 'tst' not in item:
        item['tst'] = int(time.time())
    if item['tst'] < 1000:
        item['tst'] = int(time.time())

    if 'tid' not in item:
        item['tid'] = topic[-2]

    # FIXME: should we remove these?
    for elem in ['trip', 'dist']:
        del(item[elem])

    item['topic']       = topic          # yes, add topic into payload

    item['_time']       = time.time()    # and a processing timestamp
    item['_uuid']       = str(uuid.uuid4())

    return item

def on_connect(mosq, userdata, flags, rc):
    
    for t in topics:
        _logger.info("subscribing to %s" % (t))
        mqttc.subscribe(t, 0)

def on_message(mosq, userdata, msg):
    bean = userdata
    topic = msg.topic
    # print("%s (qos=%s, r=%s) %s" % (msg.topic, str(msg.qos), msg.retain, str(msg.payload)))

    if msg.retain:
        return

    try:
        item = payload2location(topic, msg.payload)
        item['uniqueid'] = topic

        try:
            j = bean.put(json.dumps(item))
            _logger.debug("job=%d into tube=%s: %s" % (j, TUBE, json.dumps(item)))
        except socket.error as e:
            print("Socket error ", str(e))

    except beanstalkc.SocketError:
            _logger.error("Beanstalkd socket error; exiting")
            sys.exit(1)

    except Exception as e:
        raise
        print("Problem here: ", topic, str(e))
        return


def on_disconnect(mosq, userdata, rc):

    reasons = {
       '0' : 'Connection Accepted',
       '1' : 'Connection Refused: unacceptable protocol version',
       '2' : 'Connection Refused: identifier rejected',
       '3' : 'Connection Refused: server unavailable',
       '4' : 'Connection Refused: bad user name or password',
       '5' : 'Connection Refused: not authorized',
    }
    reason = reasons.get(rc, "code=%s" % rc)
    print("Disconnected: ", reason)
    _logger.info("Disconnected: %s", reason)

clientid = 'mqtt2bean-input-%s' % os.getpid()
protocol=paho.MQTTv31  # 3
protocol=paho.MQTTv311 # 4

beanstalk = beanstalkc.Connection('127.0.0.1', 11300)
beanstalk.use(TUBE)

mqttc = paho.Client(clientid, clean_session=True, userdata=beanstalk, protocol=protocol)
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect

if len(cafile) is not 0:
    mqttc.tls_set(cafile, cert_reqs=ssl.CERT_REQUIRED)

if len(username) is not 0 or len(password) is not 0:
    mqttc.username_pw_set(username, password)

_logger.info("connecting to %s:%d" % (host, port))
mqttc.connect(host, port, 60)

while True:
    try:
        mqttc.loop_forever()
    except socket.error:
        print("MQTT server disconnected; sleeping")
        _logger.info("MQTT server disconnected; sleeping")
        time.sleep(5)
    except KeyboardInterrupt:
        mqttc.disconnect()
        sys.exit(0)
    except:
        raise

