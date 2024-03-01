#####################################################
# MQTT Stuff
#####################################################

import logging
import re
import time

import network
import uasyncio as asyncio
import ujson as json
from umqtt.simple import MQTTClient, MQTTException

from config import (HOST_NAME, LAST_WILL_MSG, LAST_WILL_TOPIC, ROOT_TOPIC,
                    TELEGRAM_TOPIC, broker, publish_as_json, publish_as_values)
from utilities import reboot
from wifi import wlan, wlan_stable

# Logging
log = logging.getLogger('mqttclient')
VERBOSE = True
#counter
_conn_errors = 0

class MQTTClient2(object):
    """
    docstring
    """
    def __init__(self):
        # TODO: broker is imported directly
        self.mqtt_client = None
        self.server = broker['server']
        self.user = broker['user']
        self.password = broker['password']
        self.ping_failed = 0
        self.port: int = int(broker['port']) if 'port' in broker else 1883
        self.lastping = 0


    def ping(self):
        "ping the server"
        fastping=False
        if self.lastping and time.ticks_ms() - self.lastping < 50:
            log.debug('ping too soon')
            Falseping=True
        self.lastping = time.ticks_ms()
        if self.mqtt_client:
            if not fastping:
                self.mqtt_client.ping()
            # drain the message queue for possible messages
            process=True
            while process:
                ret = self.mqtt_client.check_msg()
                process = ret is not None
                #log.debug(f"check_msg returned {ret }")
            self.lastping = time.ticks_ms()
        else:
            log.warning('no mqtt client to ping')

    def healthy(self) -> bool:
        "is the client healthy?"
        state = True
        try:
            if wlan.status() != network.STAT_GOT_IP:
                log.debug('wlan.status != GOT_IP')
                state = False
            elif not self.mqtt_client:
                log.debug('mqtt_client = None')
                state = False
            elif self.mqtt_client.sock is None:
                log.debug('mqtt_client.sock = None')
                state = False
            else:
                # do server ping
                try:
                    self.ping()
                    self.ping_failed = 0
                except (OSError, MQTTException) as e:
                    log.warning(f'mqtt_client.ping() failed: {e}')
                    self.ping_failed =+ 10
                    if self.ping_failed > 50:
                        log.debug("Disconnecting due to ping fail count")
                        self.disconnect()

        except (OSError, MQTTException) as e:
            log.debug("error during health check: {} {}".format(type(e).__name__, e ) )
            if type(e) is type(OSError()):
                if e.args[0] == 128: # Socket error reported on server
                    log.debug("Disconnecting")
                    self.disconnect()

            state = False

        if not state:
            if VERBOSE:
                log.warning('mqtt not healthy')
            # todo: trigger reconnect ?
        return state

    def disconnect(self):
        "disconnect and close"
        if self.mqtt_client:
            log.debug('disconnecting from MQTT server')
            try:
                self.publish_one(LAST_WILL_TOPIC, LAST_WILL_MSG, retain=True)
                self.mqtt_client.disconnect()
            except BaseException as error:
                log.error("Oops while disconnecting MQTT : {}".format(error) )
            finally:
                # flag re-init of MQTT client
                self.mqtt_client = None

    def sub_cb(self, topic, msg):
        "callback for subscription"
        log.debug("Received: {} -> {}".format(topic, msg))
        if len (msg) > 0 and  "reboot" in msg.decode():
            self.disconnect()
            reboot(1)

    def connect(self):
        global _conn_errors
        if wlan.status() == network.STAT_GOT_IP:
            try:
                if self.mqtt_client is None:
                    log.info("Create MQTT client {0}".format(self.server))
                    self.mqtt_client = MQTTClient(client_id=HOST_NAME, server=self.server, port=self.port, user=self.user, password=self.password, keepalive=30 )
                if LAST_WILL_MSG and LAST_WILL_TOPIC:
                    self.mqtt_client.set_last_will(LAST_WILL_TOPIC, LAST_WILL_MSG, retain=True)
                    log.info("Set last will to {0} -> {1}".format(LAST_WILL_TOPIC, LAST_WILL_MSG))
                self.mqtt_client.set_callback(self.sub_cb)
                log.info("Connecting to MQTT server {0}".format(self.server))
                self.mqtt_client.connect(clean_session=True)
                log.info("Connected to MQTT server {0}".format(self.server))
                self.mqtt_client.subscribe(ROOT_TOPIC + b"/cmd")
                log.info("MQTT subscribed to {0}".format(ROOT_TOPIC + b"/cmd"))
                self.publish_one(LAST_WILL_TOPIC, 'online',retain=True)
            except (MQTTException, OSError)  as e:
                # try to give a decent error for common problems
                if type(e) is type(MQTTException()):
                    if e.args[0] == 5: # EIO
                        log.error("MQTT server error {}: {}".format(e, "check username/password"))
                    elif e.args[0] == 2: # ENOENT
                        log.error("MQTT server error {}: {}".format(e, "server address or network"))
                    else:
                        log.error("{} {}".format(type(e).__name__, e ) )
                else:
                    ## OSError
                    if int(e.args[0]) == -2: # could not resolve ?
                        log.error("OS Error {}: {}".format(e, "Host unreachable, is mDNS working to resolve MQTT server name ?"))
                    elif e.args[0] in (113, 23) : # EHOSTUNREACH
                        log.error("OS Error {}: {}".format(e, "Host unreachable, check server address or network"))
                    elif e.args[0] < 0 : # some negative socket error
                        _conn_errors =+ 1
                        if _conn_errors > 10:
                            log.error("OS Error {}: {}".format(e, "attempting reboot to fix"))
                            reboot()
                        else:
                            log.error("OS Error {}".format(e))
                    else:
                        log.error("{} {}".format(type(e).__name__, e ) )
        else:
            log.warning('network not ready/stable')

    async def ensure_mqtt_connected(self):
        # also try/check for OSError: 118 when connecting, to avoid breaking the loop
        # repro: machine.reset()
        while True:
            try:
                if not wlan.status()==network.STAT_GOT_IP or self.mqtt_client is None or self.mqtt_client.sock is None: 
                    log.warning('need to start MQTT client')
                    self.connect()
                else:
                    self.ping()
            except Exception as e:
                log.error("Problem during ensure_mqtt_connected: {}".format(e))
            await asyncio.sleep(10)

    async def publish_history(self, history: dict, history_topic:str) -> bool:
        if publish_as_json:
            #write readings as json
            topic = ROOT_TOPIC + b"/" + history_topic.encode()
            if not self.publish_one(topic, json.dumps(history)):
                log.warning("Could not published {history_topic} history as json")
                return False
            log.debug(f"Published {history_topic} history as json")
        return True

    async def publish_telegram(self, readings: dict) -> bool:
        if publish_as_json:
            #write readings as json
            topic = ROOT_TOPIC + b"/" + TELEGRAM_TOPIC
            if not self.publish_one(topic, json.dumps(readings)):
                log.warning("Could not publish {} meter readings as json".format(len(readings)))
                return False
            log.debug("Published {} meter readings as json".format(len(readings)))

        #write readings 1 by one
        if publish_as_values:
            for  key, value in readings.items():
                topic = ROOT_TOPIC + b"/"+ key.encode()
                if not self.publish_one(topic, str(value)):
                    log.warning("Could not publish {} meter readings value {}".format(key,value))
                    return False
            log.info("Published {} meter readings".format(len(readings)))
        return True
    
    async def publish_readings(self, readings: list) -> bool:
        if publish_as_json:
            #write readings as json
            topic = ROOT_TOPIC + b"/json"
            if not self.publish_one(topic, json.dumps(readings)):
                log.warning("Could not publish {} meter readings as json".format(len(readings)))
                return False
            log.debug("Published {} meter readings as json".format(len(readings)))

        #write readings 1 by one
        for meter in readings:
            topic = ROOT_TOPIC + b"/"+ meter['meter'].encode()
            if not self.publish_one(topic, meter['reading']):
                log.warning("Could not publish {} meter readings".format(len(readings)))
                return False
        log.info("Published {} meter readings".format(len(readings)))
        return True

    def publish_one(self, topic, value, retain=False) -> bool:
        "Publish a single ROOT_TOPIC to MQTT"
        if not self.healthy():
            return False
        r = True
        try:
            self.mqtt_client.publish(topic, value, retain)
        except BaseException as error:
            log.error("Problem sending {} to MQTT : {}".format(topic, error) )
            r = False
            self.disconnect()
        return r
