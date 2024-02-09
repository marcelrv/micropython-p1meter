import logging

import uasyncio as asyncio
import ujson as json  # used for deepcopy of dict
import ure as re
import utime as time
from machine import UART, Pin

import config as cfg
from mqttclient import MQTTClient2
from utilities import Feedback, crc16, seconds_between

# Logging
log = logging.getLogger('p1meter')
#set level no lower than ..... for this log only
#log.level = min(logging.DEBUG, logging._level) #pylint: disable=protected-access
VERBOSE = True

print(r"""
______  __   ___  ___     _            
| ___ \/  |  |  \/  |    | |           
| |_/ /`| |  | .  . | ___| |_ ___ _ __ 
|  __/  | |  | |\/| |/ _ \ __/ _ \ '__|
| |    _| |_ | |  | |  __/ ||  __/ |   
\_|    \___/ \_|  |_/\___|\__\___|_|     v 1.3.0
""")

if cfg.RUN_SPLITTER:
    print(r"""
     __  ___  _    _  ___  ___  ___  ___ 
    / _|| o \| |  | ||_ _||_ _|| __|| o \
    \_ \|  _/| |_ | | | |  | | | _| |   /
    |__/|_|  |___||_| |_|  |_| |___||_|\\
    """)

def dictcopy(d: dict):
    "returns a copy of a dict using copy though json"
    return json.loads(json.dumps(d))


# @timed_function
def replace_codes(readings: list)-> list:
    "replace the OBIS codes by their ROOT_TOPIC as defined in the codetable"
    result = []
    for reading in readings:
        for code in cfg.codetable:
            if re.match(code[0], reading['meter']):
                reading['meter'] = re.sub(code[0], code[1], reading['meter'])

                #if reading['unit'] and len(reading['unit']) > 0:
                #    reading['meter'] += '_' + reading['unit']
                if VERBOSE:
                    log.debug("{} --> {}".format(code[0], reading['meter']))
                result.append(reading)        
                break
    return result

class P1Meter():
    """
    P1 meter to take readings from a Dutch electricity meter and publish them on mqtt for consumption by homeassistant
    """
    cts: Pin
    dtr: Pin
    fb: Feedback
    uart: UART

    def __init__(self, mq_client: MQTTClient2, fb: Feedback):
        # init port for receiving 115200 Baud 8N1 using inverted polarity in RX/TX
        # UART 1 = Receive and TX if configured as Splitter
        self.uart = UART(1, rx=cfg.RX_PIN_NR, tx=cfg.TX_PIN_NR,
                         baudrate=115200, bits=8, parity=None,
                         stop=1, #invert=UART.INV_RX | UART.INV_TX,
                         invert=0,
                         txbuf=2048, rxbuf=2048)                     # larger buffer for testing and stability
        log.info("setup to receive P1 meter data : {}".format(self.uart))
        self.last = []
        self.pending = {}
        self.last_time = time.localtime()
        self.message = ''
        self.telegrams_rx = 0
        self.telegrams_tx = 0
        self.telegrams_pub = 0
        self.telegrams_err = 0
        self.mqtt_client = mq_client
        self.fb = fb
        self.crc_received = ''
        # receive set CTS/RTS High
        self.cts = Pin(cfg.CTS_PIN_NR, Pin.OUT)
        self.cts.on()                 # Ask P1 meter to send data
        # In case the
        self.dtr = Pin(cfg.DTR_PIN_NR, Pin.IN, Pin.PULL_DOWN)
        with open('obis.json') as fp:
            self.obis:list = json.load(fp)["obis_fields"]
        self.daily_usage:dict | None = None


    def clearlast(self)-> None:
        "trigger sending the complete next telegram by forgetting the previous"
        if len(self.last) > 0:
            log.warning("trigger sending the complete next telegram by forgetting the previous")
            self.last = []
            self.fb.update(Feedback.LED_P1METER, Feedback.PURPLE)

    async def receive(self):
        "Receive telegrams from the p1 meter and send them once received"
        sreader = asyncio.StreamReader(self.uart)
        #start with an empty telegram, explicit to avoid references
        empty = {'header': '', 'data': [], 'footer': ''}
        tele = dictcopy(empty)
        log.info("listening on UART1 RX Pin:{} for P1 meter data".format(cfg.RX_PIN_NR))
        if cfg.RUN_SPLITTER:
            log.info("repeating on UART1 RX Pin:{} ".format(cfg.TX_PIN_NR))
        while True:
            line = await sreader.readline()         #pylint: disable= not-callable
            if VERBOSE:
                log.debug("raw: {}".format(line))
            if line:
                # to string
                try:
                    line = line.decode()
                except BaseException as error:      #pylint: disable= unused-variable
                    line = "--noise--"
                if VERBOSE:
                    log.debug("clean : {}".format(line))
                if line[0] == '/':
                    log.debug('header found')
                    self.fb.update(Feedback.LED_P1METER, Feedback.GREEN)
                    tele = dictcopy(empty)
                    self.message = line

                elif line[0] == '!':
                    log.debug('footer found')
                    tele['footer'] = line
                    self.message += "!"
                    if len(line) > 5:
                        self.crc_received = line[1:5]
                    # self.message += line
                    # Process the received telegram
                    await self.process(tele)
                    # start with a blank slate
                    self.message = ''
                    self.crc_received = ''

                elif line != "--noise--":
                    tele['data'].append(line)
                    # add to message
                    self.message += line

    @property
    def crc(self) -> str:
        "Compute the crc of self.message"
        buf = self.message.encode()
        # TMI log.debug( "buf: {}".format(buf))
        return "{0:04X}".format(crc16(buf))


    def crc_ok(self, tele: dict = None)-> bool:
        "run CRC-16 check on the received telegram"
        if not tele or not self.message:
            return False
        try:
            # cache crc to avid wasting time
            crc = self.crc
            log.debug("RX computed CRC {0}".format(crc))
            if crc in tele['footer']:
                return True
            else:
                log.warning("CRC Failed, computed: {0} != received {1}".format(str(crc), str(tele['footer'][1:5])))
                return False
        except (OSError, TypeError) as e:
            log.error("Error during CRC check: {}".format(e))
            return False

    def parsereadings(self, newdata):
        "split the received data into readings(meter, reading, unit)"
        readings = []
        for line in newdata:
            out = re.match('(.*?)\((.*)\)', line)           #pylint: disable=anomalous-backslash-in-string
            if out:
                lineinfo = {'meter': out.group(1), 'reading':None, 'unit': None}

                reading = out.group(2).split('*')
                if len(reading) == 2:
                    lineinfo['reading'] = reading[0]
                    lineinfo['unit'] = reading[1]
                else:
                    lineinfo['reading'] = reading[0]
                # a few meters have compound content, that remain seperated by `)(`
                # split and use  only the last section (ie gas meter reading)
                lineinfo['reading'] = lineinfo['reading'].split(')(')[-1]
                if VERBOSE:
                    log.debug(lineinfo)
                readings.append(lineinfo)
        return readings

    def parse_obis_telegram(self,telegram) -> Dict:
        def parse_hex(str) -> str:
            try:
                result = bytes.fromhex(str).decode()
            except ValueError:
                result = str
            return result
        def format_value(value: str, type: str, unit: str) -> Union[str, float]:
            # Timestamp has message of format "YYMMDDhhmmssX"
            multiply = 1
            if len(unit) > 0 and unit[0] == "k":
                multiply = 1000
            format_functions: dict = {
                "float": lambda str: float(str) * multiply,
                "int": lambda str: int(str) * multiply,
                "timestamp": lambda str: '20' + str[:2] + '-' + str[2:4] + '-' + str[4:6] + ' ' + str[6:8] + ':' + str[8:10] + ':' + str[10:12] ,
                "string": lambda str: parse_hex(str),
                "unknown": lambda str: str,
            }
            return_value = format_functions[type](value.split("*")[0])
            return return_value

        telegram_formatted: dict = {}
        for line in telegram:
            obis_key = line['meter'].strip()
            obis_item = None
            for item in self.obis:
                if item.get("key","") == obis_key:
                    obis_item = item
                    break
            if obis_item is not None:
                item_type: str = obis_item.get("type", "")
                # logger.debug("Key %s  Name: %s Unit: %s <-- %s" %  (obis_item.get("key", "") ,  obis_item.get("name", "") , obis_item.get("unit", "no unit") , line. strip()) )
                unit = obis_item.get("unit", "no unit")
                telegram_formatted[obis_item.get("name")] = (
                    format_value(line['reading'], item_type, unit)
                )
        return telegram_formatted



    def post_process(self, telegram_formatted: dict) -> dict:
        "post process the telegram for  (net) totals and daily usage etc"
        telegram_formatted["electricityImported"] = (
        telegram_formatted["electricityImportedT1"] * 1 + telegram_formatted["electricityImportedT2"] * 1)
        telegram_formatted["electricityExported"] = (
        telegram_formatted["electricityExportedT1"] * 1 + telegram_formatted["electricityExportedT2"] * 1)
        telegram_formatted["electricityImportedNet"] = ( telegram_formatted["electricityImported"] * 1 - telegram_formatted["electricityExported"] * 1)
        telegram_formatted["powerNetActual"] = (
        telegram_formatted["powerImportedActual"] * 1 - telegram_formatted["powerExportedActual"] * 1) 
        return telegram_formatted

    def daily_totals(self, telegram_formatted: dict) -> dict:
        "calculate the daily usage"
        daily_usage = self.daily_usage
        daily_usage_fields=[ "electricityImported", "electricityExported", "electricityImportedNet"]
        daily_usage_file = f"/daily/{telegram_formatted["timestamp"][:7]}_daily_usage.json"
        monthly_usage = {}
        if daily_usage is None:
            log.info(f"Reading daily values from {daily_usage_file}")
            try:
                with open(daily_usage_file, "r") as file:
                    monthly_usage = json.load(file)
                daily_usage = monthly_usage[telegram_formatted["timestamp"][:10]]
                self.daily_usage = daily_usage
            except Exception as e:
                log.error(f"Error reading daily values from {daily_usage_file}: {e}")
    
        # Store the daily usage in a separate dictionary
        if daily_usage is None or (daily_usage["timestamp"][:10] != telegram_formatted["timestamp"][:10]):
            log.info(f"Updating daily values and save to {daily_usage_file}")
            daily_usage = dict()
            for field in daily_usage_fields:
                try:
                    daily_usage[field] = telegram_formatted[field]
                except KeyError:
                    daily_usage[field] = 0
                    telegram_formatted[field] = 0
                    log.error(f"Field {field} not found in telegram")
            daily_usage["timestamp"] = telegram_formatted["timestamp"]
            self.daily_usage = daily_usage
            monthly_usage[telegram_formatted["timestamp"][:10]] = daily_usage
            self.mqtt_client.publish_history(monthly_usage, f"hist-{telegram_formatted["timestamp"][:7]}")
            try:
                with open(daily_usage_file, "w") as file:
                    file.write(json.dumps(monthly_usage))
            except Exception as e:
                log.error(f"Error writing daily values to {daily_usage_file}: {e}")
        for field in daily_usage_fields:
            telegram_formatted[f"{field}Today"] = telegram_formatted[field] - daily_usage[field]
        return telegram_formatted    

    async def process(self, tele: dict):
        # check CRC
        if not self.crc_ok(tele):
            self.telegrams_err += 1
            self.fb.update(Feedback.LED_P1METER, Feedback.RED)
            return
        else:
            self.fb.update(Feedback.LED_P1METER, Feedback.GREEN)
            self.telegrams_rx += 1

        # Send a copy of the received message (self.message)
        if cfg.RUN_SPLITTER:
            await self.send(self.message)
            self.telegrams_tx += 1

        # what has changed since last time  ?
        #newdata = set(tele['data']) - set(self.last)
        #readings = self.parsereadings(newdata)        
        readings = self.parsereadings(tele['data'])

        telegram = self.parse_obis_telegram(readings)
        telegram = self.post_process(telegram)
        telegram = self.daily_totals(telegram)
        log.debug("telegram: {}".format(telegram))
 
        # move list into dictionary
#        for reading in readings:
#            self.pending[reading['meter']] = reading

        delta_sec = seconds_between(self.last_time, time.localtime())
        if  delta_sec < cfg.INTERVAL_MIN and self.telegrams_pub > 0:
            ## do not send too often, remember any changes to send later
            log.info('suppress send')
            log.debug('pending : {}'.format(self.pending.keys))
            # turn off
            self.fb.update(Feedback.LED_P1METER, Feedback.BLACK)
        else:
            # send this data and any unsent information
            self.fb.update(Feedback.LED_P1METER, Feedback.GREEN)
            #readings = list(self.pending.values())

            if await self.mqtt_client.publish_telegram(telegram):
                # only safe last if mqtt publish was ok
                self.telegrams_pub += 1
                #self.last = tele['data'].copy()
                self.pending = {}
                self.last_time = time.localtime()
            else:
                self.fb.update(Feedback.LED_MQTT, Feedback.YELLOW)
            # Turn off

    async def send(self, telegram: str):
        """
        Sends/repeats telegram, with added CRC16
        """

        log.info('Copy telegram')
        if not self.dtr.value():
            log.warning("Splitter DTR is Low, will not send P1 telegram data")
        else:
            swriter = asyncio.StreamWriter(self.uart, {})

            self.fb.update(Feedback.LED_P1METER, Feedback.BLUE)
            if VERBOSE:
                log.debug(b'TX telegram message: ----->')
                log.debug(telegram)
                log.debug(b'-----')
            swriter.write(telegram + self.crc_received  + '\r\n')
            await swriter.drain()       # pylint: disable= not-callable
            await asyncio.sleep_ms(1)


