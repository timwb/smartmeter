#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Read messages from a DSMR smart meter P1 port, parse, and submit to influxdb and MQTT broker
# Compute cost for gas and electricity according to Dutch cost structure
# Keep stats of 5m average, usage of today, yesterday an the day before. Keep state across restarts.
#TODO translate some Dutch comments to English
import sys, os, argparse, logging, logging.handlers, json, yaml, signal
import time, serial, re, collections
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from datetime import date, datetime
import libscrc, math

configfile  = '/etc/smartmeter/smartmeter-config.yaml'
datafile    = '/dev/shm/smartmeter.json'
statefile   = '/var/local/smartmeterstate.json'
hostname    = os.uname()[1]
discovery_published = False

class cl_data:
  def __init__(self):
    # json file in /dev/shm
    self._datafile = open(datafile, 'w+')

  def close(self):
    self._datafile.close()

  def writedata(self, m, stats, t):
    self._datafile.seek(0, 0)
    jsondata = {}
    jsondata['lastmessage'] = m
    jsondata['stats'] = stats.stats
    jsondata['telegram'] = t
    json.dump(jsondata, self._datafile)
    self._datafile.truncate()
    self._datafile.flush()


# Stats are derived. Some is updated with every telegram, others only daily
# Keep it in one big dictionary, with some functions to initialize and update
class cl_stats:
  def __init__(self):
    if config['dsmrversion'] == '5': # version 5 reads every second, so compute the sliding 5m average over 300 samples. This assumes no telegrams get dropped.
      self._pwrlog = collections.deque(maxlen=300)
    else:
      self._pwrlog = collections.deque(maxlen=30)
    self._prevtts = 0
    self._prevgts = 0
    self._prevgdl = 0
    self.updategas = False
    self.enextday = False
    self.gnextday = False
    # 2 is normal rate, 1 is off-peak rate
    self.stats = {
      'electricity': {
        'realtime': {
          '5mavgpwr':   0.0,
          'et1today':   0,
          'et2today':   0,
          'et1now':     0,
          'et2now':     0,
        },
        'daily': {
          'et1yesterday':  0,
          'et2yesterday':  0,
          'et1daybefore':  0,
          'et2daybefore':  0,
          'edt1atmidnight': 0,
          'edt2atmidnight': 0,
        }
      },
      'gas': {
        'realtime': {
          'gflowrate':  0.0,
          'gtoday':     0,
        },
        'daily': {
          'gyesterday':  0,
          'gdaybefore':  0,
          'gdatmidnight': 0,
        }
      },
      'lastmidnightts':    0,
      'lastnumrmessage':   0,
      'lastnumrmessagets': 0,
      'lasttextmessage':   '',
      'lasttextmessagets': 0,
    }

    # Read stats from disk and parse if valid
    try:
      with open(statefile, 'r') as jsonstatefile:
        diskstats = json.load(jsonstatefile)
    except (FileNotFoundError, json.JSONDecodeError):
      logger.info("State file missing or invalid, state not restored")
      return
    logger.info("State file loaded")

    self.stats['lastmidnightts'] = diskstats.get('lastmidnightts', 0)
    if 'electricity' in diskstats and 'gas' in diskstats:
      if 'daily' in diskstats['electricity'] and 'daily' in diskstats['gas']:
        logger.debug("State file appears valid")
      else:
        logger.warning("State file corrupt, daily missing")
        return
    else:
      logger.warning("State file corrupt, electricity or gas missing")
      return

    # TODO Stats of yesterday and day before were only for the oled display. Probably better to keep this there or get it from a database.
    # Strictly speaking the only state we need here is the meter readings at midnight, to compute daily totals.
    # TODO remove:3660 hack Probably something is going wrong with UTC vs CET
    if self.stats['lastmidnightts'] != 0 and datetime.fromtimestamp(time.time()).strftime('%y%m%d') == datetime.fromtimestamp(self.stats['lastmidnightts']+3660).strftime('%y%m%d'):
      logger.info("State recent, continuing where we left off")
      self.stats['electricity']['daily']['et1yesterday']   = diskstats['electricity']['daily'].get('et1yesterday',   0)
      self.stats['electricity']['daily']['et2yesterday']   = diskstats['electricity']['daily'].get('et2yesterday',   0)
      self.stats['electricity']['daily']['et1daybefore']   = diskstats['electricity']['daily'].get('et1daybefore',   0)
      self.stats['electricity']['daily']['et2daybefore']   = diskstats['electricity']['daily'].get('et2daybefore',   0)
      self.stats['electricity']['daily']['edt1atmidnight'] = diskstats['electricity']['daily'].get('edt1atmidnight', 0)
      self.stats['electricity']['daily']['edt2atmidnight'] = diskstats['electricity']['daily'].get('edt2atmidnight', 0)
      self.stats['gas']['daily']['gyesterday']   = diskstats['gas']['daily'].get('gyesterday',   0)
      self.stats['gas']['daily']['gdaybefore']   = diskstats['gas']['daily'].get('gdaybefore',   0)
      self.stats['gas']['daily']['gdatmidnight'] = diskstats['gas']['daily'].get('gdatmidnight', 0)
    else:
      logger.info("State data is stale, ignoring")
      self.stats['lastmidnightts'] = 0.0

    self.stats['lastnumrmessage']   = diskstats.get('lastnumrmessage',   0)
    self.stats['lastnumrmessagets'] = diskstats.get('lastnumrmessagets', 0)
    self.stats['lasttextmessage']   = diskstats.get('lasttextmessage',   '')
    self.stats['lasttextmessagets'] = diskstats.get('lasttextmessagets', 0)

  def writestate(self):
    with open(statefile, 'w') as jsonstatefile:
      logger.debug("Writing new state data")
      json.dump(self.stats, jsonstatefile)
      jsonstatefile.truncate()
      jsonstatefile.flush()

  # Compute stats, 24h counters
  def updatestats(self, m):
    self.updategas = False
    self.enextday = False
    self.gnextday = False
    # Compute 5-minute sliding window average
    self._pwrlog.append(m['pdw']-m['prw'])
    self.stats['electricity']['realtime']['5mavgpwr'] = 0
    for p in self._pwrlog:
      self.stats['electricity']['realtime']['5mavgpwr'] += p / len(self._pwrlog)

    self.stats['electricity']['realtime']['et2now'] = m['edt2'] - m['ert2']
    self.stats['electricity']['realtime']['et1now'] = m['edt1'] - m['ert1']

    # Compute current through neutral using the low-res in-meter current measurement as well as computed current
    # cn2 Assumes pf=1 so this is only an approximation!
    self.stats['electricity']['realtime']['cn'] = currentthroughneutral([m['cl1'], m['cl2'], m['cl3']])
    self.stats['electricity']['realtime']['cn2'] = currentthroughneutral([
      (m['pdwl1'] - m['prwl1']) / m['vl1'] if m['vl1'] else 0,
      (m['pdwl2'] - m['prwl2']) / m['vl2'] if m['vl2'] else 0,
      (m['pdwl3'] - m['prwl3']) / m['vl3'] if m['vl3'] else 0,
    ])

    if 'gdl' in m:
      if self._prevgdl != 0:
        if m['gts'] > self._prevgts:
          self.stats['gas']['realtime']['gflowrate'] = (m['gdl'] - self._prevgdl) / (m['gts'] - self._prevgts)
          logger.debug(f"Updating previous gas delivered value from {self._prevgdl} to {m['gdl']}")
          self._prevgdl = m['gdl']
          self.updategas = True
          if self.stats['gas']['daily']['gdatmidnight'] != 0:
            self.stats['gas']['realtime']['gtoday']  = m['gdl'] - self.stats['gas']['daily']['gdatmidnight']
      else:
        self._prevgdl = m['gdl']
        logger.debug(f"Initializing previous gas delivered value to: {self._prevgdl}")

    # Update stats
    if self.stats['electricity']['daily']['edt1atmidnight'] != 0:
      self.stats['electricity']['realtime']['et1today'] = self.stats['electricity']['realtime']['et1now'] - self.stats['electricity']['daily']['edt1atmidnight']
      self.stats['electricity']['realtime']['et2today'] = self.stats['electricity']['realtime']['et2now'] - self.stats['electricity']['daily']['edt2atmidnight']

    if 'numrmessage' in m and m['numrmessage'] != 0:
      self.stats['lastnumrmessage']   = m['numrmessage']
      self.stats['lastnumrmessagets'] = m['ts']

    if 'textmessage' in m and m['textmessage'] != '':
      self.stats['lasttextmessage']   = m['textmessage']
      self.stats['lasttextmessagets'] = m['ts']

    # If a new day started, rotate daily totals and reset today's back to 0
    if self._prevtts != 0 and datetime.fromtimestamp(m['ts']).strftime('%d') != datetime.fromtimestamp(self._prevtts).strftime('%d'):
      logger.debug("It's a brand new day for electricity!")
      self.stats['electricity']['daily']['et2daybefore'] = self.stats['electricity']['daily']['et2yesterday']
      self.stats['electricity']['daily']['et1daybefore'] = self.stats['electricity']['daily']['et1yesterday']

      if self.stats['electricity']['daily']['edt1atmidnight'] != 0:
        self.stats['electricity']['daily']['et1yesterday'] = self.stats['electricity']['realtime']['et1today']
        self.stats['electricity']['daily']['et2yesterday'] = self.stats['electricity']['realtime']['et2today']
        #self.stats['electricity']['daily']['ecyesterday'] =  self.stats['electricity']['realtime']['ectoday']
      else:
        logger.debug("First new day for electricity, no prior usage data")

      self.stats['electricity']['daily']['edt2atmidnight'] = self.stats['electricity']['realtime']['et2now']
      self.stats['electricity']['daily']['edt1atmidnight'] = self.stats['electricity']['realtime']['et1now']
      self.stats['electricity']['realtime']['et1today'] = 0
      self.stats['electricity']['realtime']['et2today'] = 0
      self.stats['lastmidnightts'] = m['ts']
      self.writestate()
      self.enextday = True

    if self._prevgts != 0 and datetime.fromtimestamp(m['gts']).strftime('%d') != datetime.fromtimestamp(self._prevgts).strftime('%d'):
      logger.debug("It's a brand new day for gas!")
      self.stats['gas']['daily']['gdaybefore']  = self.stats['gas']['daily']['gyesterday']

      if self.stats['gas']['daily']['gdatmidnight'] != 0:
        self.stats['gas']['daily']['gyesterday'] =  self.stats['gas']['realtime']['gtoday']
      else:
        logger.debug("First new day for gas, no prior usage data")

      self.stats['gas']['daily']['gdatmidnight'] = m['gdl']
      self.stats['gas']['realtime']['gtoday']  = 0
      self.writestate()
      self.gnextday = True

    self._prevtts = m['ts']
    if 'gts' in m: self._prevgts = m['gts']


# Keys averaged over the HA publish interval; the rest take the last value.
_HA_AVG_M_KEYS    = {'pdw', 'prw', 'vl1', 'vl2', 'vl3', 'cl1', 'cl2', 'cl3'}
_HA_AVG_STAT_KEYS = {'5mavgpwr', 'cn', 'cn2'}
_HA_LAST_M_KEYS   = {'edt1', 'edt2', 'ert1', 'ert2', 'tariff'}


class cl_ha_publisher:
  """Throttle MQTT publishes to Home Assistant for DSMR5 (1 telegram/s).

  Accumulates values over `interval` telegrams, then publishes averaged
  measurements and the last-seen counter values to <base>/<eqid>/ha/<key>.
  """
  def __init__(self, interval=10):
    self._interval = interval
    self._reset()

  def _reset(self):
    self._count = 0
    self._sums  = {k: 0.0 for k in _HA_AVG_M_KEYS | _HA_AVG_STAT_KEYS}
    self._last  = {}

  def accumulate(self, m, stats):
    for k in _HA_AVG_M_KEYS:
      if k in m:
        self._sums[k] += m[k]
    for k in _HA_AVG_STAT_KEYS:
      val = stats.stats['electricity']['realtime'].get(k)
      if val is not None:
        self._sums[k] += val
    for k in _HA_LAST_M_KEYS:
      if k in m:
        self._last[k] = m[k]
    self._count += 1

    if self._count >= self._interval:
      self._publish(m['eqid'])
      self._reset()

  def _publish(self, eqid):
    topic = config['mqtt']['topic'] + '/' + eqid + '/ha'
    n = self._count
    for k in _HA_AVG_M_KEYS | _HA_AVG_STAT_KEYS:
      mqttclient.publish(topic + '/' + k, payload=str(round(self._sums[k] / n, 3)))
    for k, v in self._last.items():
      mqttclient.publish(topic + '/' + k, payload=str(v))
    logger.debug(f"Published HA-throttled data to {topic}")


#TODO Implement actually reloading the config.
def reloadconfig(signum, frame):
    global discovery_published
    logger.info(f'Received signal {signum}')
    discovery_published = False


def shutdown(signum, frame):
    logger.info(f'Received signal {signum}')
    if 'stats' in dir(): stats.writestate()
    data.close()


def on_connect(client, userdata, flags, rc):
  logger.info(f"MQTT client connected with result code {rc}")


def currentthroughneutral(c=[0, 0, 0]):
  c = sorted(c, reverse=True)
  if c[1] == 0 and c[2] == 0: #we're running on a single phase
    return c[0]
  # Make sure the math keeps working when one or more phases feed power back
  if c[2] < 0:
   c[0] += abs(c[2])
   c[1] += abs(c[2])
   c[2] = 0
  if c[0] == 0 or c[1] == 0:
    return c[2]
  i1 = math.sqrt((c[0]**2 + c[1]**2) - (c[0] * c[1]))
  cosb = (( i1**2 + c[1]**2 ) - c[0]**2 ) / ( 2 * c[0] * c[1] )
  cosd = math.cos( math.acos(cosb) - math.radians(60) )
  cn = math.sqrt(( c[2]**2 + i1**2 ) - ( cosd * 2 * c[2] * i1 ))
  return cn


# Decode telegram t in to message m
def decodetelegram(t, tts):
  m = {}
  res = {}
  n = -1
  # validate, build dictionary from results (res) and parse to message (m)
  # results is a dictionary of OBIS reference code and a tuple of value and line number
  for l in t:
    n = n+1
    if n == 0:
      m['identification'] = l[1:]
    else:
      tmp = tcode.findall(l)
      if tmp is not None and len(tmp) != 0:
        res[tmp[0][0]] = (tmp[0][1], n)
  # edt1 - electricity delivered, tariff 1 (in kWh)
  # ert2 - electricity returned, tariff 2, etc.
  # pdw - power delivered in watt
  # gdl - gas delivered in litres
  # npf - number of power failures, nlpf long power failures (in any phase)
  # vsal1,2,3 - Voltage sags in phase L1, 2, 3. vswl1, etc is voltage swells
  if '1-3:0.2.8'   in res: m['ver'] = res['1-3:0.2.8'][0]
  if '0-0:1.0.0'   in res:
    #TODO The last letter in the timestamp is a S or W for summer or winter (standard) time.
    #Ignoring it for now, which means assuming the system time zone is the same as the grid operator's time zone.
    m['ts'] = int(datetime.strptime(res['0-0:1.0.0'][0][:-1], '%y%m%d%H%M%S').timestamp())
  else:
    m['ts'] = tts
  if '0-0:96.1.1'  in res: m['eqid'] = bytes.fromhex(res['0-0:96.1.1'][0]).decode('ASCII').strip()
  if '1-0:1.8.1'   in res: m['edt1'] = int(float(res['1-0:1.8.1'][0].replace("*kWh",""))*1000)
  if '1-0:1.8.2'   in res: m['edt2'] = int(float(res['1-0:1.8.2'][0].replace("*kWh",""))*1000)
  if '1-0:2.8.1'   in res: m['ert1'] = int(float(res['1-0:2.8.1'][0].replace("*kWh",""))*1000)
  if '1-0:2.8.2'   in res: m['ert2'] = int(float(res['1-0:2.8.2'][0].replace("*kWh",""))*1000)
  # 1 is off-peak tariff, 2 is normal tariff
  if '0-0:96.14.0' in res: m['tariff'] =     int(res['0-0:96.14.0'][0])
  if '1-0:1.7.0'   in res: m['pdw'] =  int(float(res['1-0:1.7.0'][0].replace("*kW", ""))*1000)
  if '1-0:2.7.0'   in res: m['prw'] =  int(float(res['1-0:2.7.0'][0].replace("*kW", ""))*1000)
  if '0-0:96.7.21' in res: m['npf'] =        int(res['0-0:96.7.21'][0])
  if '0-0:96.7.9'  in res: m['nlpf'] =       int(res['0-0:96.7.9'][0])
  #res['1-0:99.97.0'] # long power failure event log
  if '1-0:32.32.0' in res: m['vsal1'] =      int(res['1-0:32.32.0'][0])
  if '1-0:52.32.0' in res: m['vsal2'] =      int(res['1-0:52.32.0'][0])
  if '1-0:72.32.0' in res: m['vsal3'] =      int(res['1-0:72.32.0'][0])
  if '1-0:32.36.0' in res: m['vswl1'] =      int(res['1-0:32.36.0'][0])
  if '1-0:52.36.0' in res: m['vswl2'] =      int(res['1-0:52.36.0'][0])
  if '1-0:72.36.0' in res: m['vswl3'] =      int(res['1-0:72.36.0'][0])
  if '0-0:96.13.0' in res:
    m['textmessage']  = bytes.fromhex(res['0-0:96.13.0'][0]).decode('ASCII').strip()
    if m['textmessage'] != '':
      logger.info(f"Received text message: {m['textmessage']}")
  if '0-0:96.13.1' in res:
    if res['0-0:96.13.1'][0] == '':
      m['numrmessage'] = 0
    else:
      m['numrmessage'] = int(res['0-0:96.13.1'][0])
      logger.info(f"Received numerical message: {m['numrmessage']}")
  if '1-0:32.7.0'  in res: m['vl1']   =     float(res['1-0:32.7.0'][0].replace("*V",""))
  if '1-0:52.7.0'  in res: m['vl2']   =     float(res['1-0:52.7.0'][0].replace("*V",""))
  if '1-0:72.7.0'  in res: m['vl3']   =     float(res['1-0:72.7.0'][0].replace("*V",""))
  if '1-0:31.7.0'  in res: m['cl1']   =       int(res['1-0:31.7.0'][0].replace("*A",""))
  if '1-0:51.7.0'  in res: m['cl2']   =       int(res['1-0:51.7.0'][0].replace("*A",""))
  if '1-0:71.7.0'  in res: m['cl3']   =       int(res['1-0:71.7.0'][0].replace("*A",""))
  if '1-0:21.7.0'  in res: m['pdwl1'] = int(float(res['1-0:21.7.0'][0].replace("*kW",""))*1000)
  if '1-0:41.7.0'  in res: m['pdwl2'] = int(float(res['1-0:41.7.0'][0].replace("*kW",""))*1000)
  if '1-0:61.7.0'  in res: m['pdwl3'] = int(float(res['1-0:61.7.0'][0].replace("*kW",""))*1000)
  if '1-0:22.7.0'  in res: m['prwl1'] = int(float(res['1-0:22.7.0'][0].replace("*kW",""))*1000)
  if '1-0:42.7.0'  in res: m['prwl2'] = int(float(res['1-0:42.7.0'][0].replace("*kW",""))*1000)
  if '1-0:62.7.0'  in res: m['prwl3'] = int(float(res['1-0:62.7.0'][0].replace("*kW",""))*1000)
  if '0-1:96.1.0'  in res: m['geqid'] = bytes.fromhex(res['0-1:96.1.0'][0]).decode('ASCII').strip()
  #res['0-1:24.1.0'] # Num devices on meter bus
  # gas per 5m
  if '0-1:24.2.1'  in res:
    gas = res['0-1:24.2.1'][0].split(')(')
    #TODO The last letter in the timestamp is a S or W for summer or winter (standard) time.
    #Ignoring it for now, which means assuming the system time zone is the same as the grid operator's time zone.
    m['gts'] = int(datetime.strptime(gas[0][:-1], '%y%m%d%H%M%S').timestamp())
    #TODO make this less ugly
    m['gdl'] = int(float(gas[1].replace("*m3",""))*1000)
    #m['gdl'] = int(float(t[res['0-1:24.2.1'][1]+1][1:-1])*1000)
  # gas per hour
  if '0-1:24.3.0'  in res:
    gas = res['0-1:24.3.0'][0].split(')(')
    m['gts'] = int(datetime.strptime(gas[0], '%y%m%d%H%M%S').timestamp())
    m['gdl'] = int(float(t[res['0-1:24.3.0'][1]+1][1:-1])*1000)
  return m


#TODO: Push more data to Influx
def writetoinflux(m, stats):
  # TODO: Parse the meter Id from the message
  data = "smartmeter,host={host},identification={identification},meter={meter},tariff={tariff} ".format(
    host=hostname,
    identification=m['identification'],
    meter=m['eqid'],
    tariff=m['tariff']
  )
  for key in m:
    # Filter out elements in the dictionary that shouldn't be written to influx as values
    if key not in ('ts', 'identification', 'eqid', 'tariff', 'geqid', 'gts', 'gdl', 'numrmessage', 'textmessage'):
      data += "{key}={value},".format(key=key, value=m[key])
  # This is too complicated to compute in Influx afterwards, so simply insert it.
  data += "cn={cn},cn2={cn2}".format(
    cn=round(stats.stats['electricity']['realtime']['cn'], 3),
    cn2=round(stats.stats['electricity']['realtime']['cn2'], 3)
  )
  #data = data[:-1]
  data += " {timestamp}".format(timestamp=int(m['ts']*1000000))

  if not args.dryrun: influxclient.write_points(data, time_precision='u', protocol='line')
  logger.debug(f"Writing data to Influx: {data}")

  if stats.updategas:
    logger.debug("Writing gas total to InfluxDB")
    data = "smartmeter,host={host},meter={meter} gdl={gdl} {timestamp}".format(
      host=hostname,
      meter=m['geqid'],
      gdl=m['gdl'],
      timestamp=m['gts']
    )
    if not args.dryrun: influxclient.write_points(data, time_precision='s', protocol='line')
    logger.debug(f"Writing data to Influx: {data}")


def publishdata(m, stats, t):
  # Electriciy
  topic = config['mqtt']['topic'] + '/' + m['eqid']
  if args.dryrun: return
  mqttclient.publish(topic+'/message', payload=json.dumps(m))
  mqttclient.publish(topic+'/stats', payload=json.dumps(stats.stats))
  mqttclient.publish(topic+'/telegram', payload=json.dumps(t))
  for stat in stats.stats['electricity']['realtime']:
      mqttclient.publish(topic+'/'+stat, payload=str(stats.stats['electricity']['realtime'][stat]))
  for stat in m:
    if stat not in ('ts', 'identification', 'eqid', 'geqid', 'gts', 'gdl', 'textmessage'):
      mqttclient.publish(topic+'/'+stat, payload=str(m[stat]))
  if 'textmessage' in m and m['textmessage'] != '':
      mqttclient.publish(topic+'/textmessage', payload=m['textmessage'])
  if stats.enextday:
    for stat in stats.stats['electricity']['daily']:
      mqttclient.publish(topic+'/'+stat, payload=str(stats.stats['electricity']['daily'][stat]), retain=True)

  if not stats.updategas: return
  # Gas
  logger.debug("Writing gas stats and message to MQTT")
  topic = config['mqtt']['topic'] + '/' + m['geqid']
  for stat in stats.stats['gas']['realtime']:
    mqttclient.publish(topic+'/'+stat, payload=str(stats.stats['gas']['realtime'][stat]), retain=True)
  for stat in m:
    if stat in ['gts', 'gdl']:
      mqttclient.publish(topic+'/'+stat, payload=str(m[stat]), retain=True)
  if stats.gnextday:
    logger.debug("Writing daily gas stats to MQTT")
    for stat in stats.stats['gas']['daily']:
      mqttclient.publish(topic+'/'+stat, payload=str(stats.stats['gas']['daily'][stat]), retain=True)


# Tuples: (key, name, unit, device_class, state_class)
# unit/device_class/state_class may be None
_ELEC_SENSORS = [
  ('pdw',      'Power delivered',           'W',  'power',   'measurement'),
  ('prw',      'Power returned',            'W',  'power',   'measurement'),
  ('edt1',     'Energy delivered tariff 1', 'Wh', 'energy',  'total_increasing'),
  ('edt2',     'Energy delivered tariff 2', 'Wh', 'energy',  'total_increasing'),
  ('ert1',     'Energy returned tariff 1',  'Wh', 'energy',  'total_increasing'),
  ('ert2',     'Energy returned tariff 2',  'Wh', 'energy',  'total_increasing'),
  ('vl1',      'Voltage L1',                'V',  'voltage', 'measurement'),
  ('vl2',      'Voltage L2',                'V',  'voltage', 'measurement'),
  ('vl3',      'Voltage L3',                'V',  'voltage', 'measurement'),
  ('cl1',      'Current L1',                'A',  'current', 'measurement'),
  ('cl2',      'Current L2',                'A',  'current', 'measurement'),
  ('cl3',      'Current L3',                'A',  'current', 'measurement'),
  ('5mavgpwr', '5-minute average power',    'W',  'power',   'measurement'),
  ('cn',       'Current through neutral',            'A', 'current', 'measurement'),
  ('cn2',      'Current through neutral (computed)', 'A', 'current', 'measurement'),
  ('tariff',   'Active tariff',             None, None,      None),
]

_GAS_SENSORS = [
  ('gdl',       'Gas total',     'L',   'gas', 'total_increasing'),
  ('gflowrate', 'Gas flow rate', 'L/s', 'volume_flow_rate', 'measurement'),
]

#TODO implement last will and testament to set all MQTT values to unavailable, and maybe also publish a retained "status" topic with "online"/"offline" or something like that.
def publish_discovery(eqid, geqid):
  global discovery_published
  prefix = config.get('ha_discovery_prefix', 'homeassistant')
  base_topic = config['mqtt']['topic']
  dsmr_model = str(config.get('dsmrversion', 'unknown'))
  # DSMR5 sends every second; HA reads from the throttled /ha/ subtopic.
  elec_state_base = f'{base_topic}/{eqid}/ha' if dsmr_model == '5' else f'{base_topic}/{eqid}'

  elec_device = {
    'identifiers': [eqid],
    'name': 'Smart Meter Electricity',
    'manufacturer': 'DSMR',
    'model': dsmr_model,
  }
  for key, name, unit, device_class, state_class in _ELEC_SENSORS:
    payload = {
      'name': name,
      'unique_id': f'{eqid}_{key}',
      'state_topic': f'{elec_state_base}/{key}',
      'device': elec_device,
    }
    if unit:         payload['unit_of_measurement'] = unit
    if device_class: payload['device_class']        = device_class
    if state_class:  payload['state_class']         = state_class
    mqttclient.publish(f'{prefix}/sensor/{eqid}_{key}/config', payload=json.dumps(payload), retain=True)

  gas_device = {
    'identifiers': [geqid],
    'name': 'Smart Meter Gas',
    'manufacturer': 'DSMR',
    'model': dsmr_model,
  }
  for key, name, unit, device_class, state_class in _GAS_SENSORS:
    payload = {
      'name': name,
      'unique_id': f'{geqid}_{key}',
      'state_topic': f'{base_topic}/{geqid}/{key}',
      'device': gas_device,
    }
    if unit:         payload['unit_of_measurement'] = unit
    if device_class: payload['device_class']        = device_class
    if state_class:  payload['state_class']         = state_class
    mqttclient.publish(f'{prefix}/sensor/{geqid}_{key}/config', payload=json.dumps(payload), retain=True)

  discovery_published = True
  logger.info("Published HA MQTT autodiscovery config")


def process_telegrams():
  #Read data from the serial port, append it until a '!' is found. Don't do anything with it until the CRC checks out.
  #bt is the binary telegram, which is assembled from pyserial readline()
  #t is the final telegram, in ASCII and split into lines
  #The simplest code would just use p1conn.read_until(b'!'). However the implementation of read_until in Pyserial is extremely CPU intensive as it checks every character coming in for a match in Python.
  l = b''
  bt = b''
  while l[0:1] != b'!':
    l = p1conn.readline()
    if l == b'':
      logger.warning("Timeout waiting for data on serial port.")
      continue
    bt += l

  if bt[0:1] != b'/':
    logger.info('Received a partial telegram, ignoring it. This can happen during startup or with a loose cable.')
    return
  try:
    crcfromt = int(l[1:6], 16)
  except ValueError:
    logger.warning(f'CRC in telegram is not hexadecimal: "{l[1:6]}"')
    return
  crccomputed = libscrc.ibm(bt[:-6])
  if crccomputed != crcfromt:
    logger.warning('CRC failed, ignoring telegram. If you see this message often, replace the cable and/or remove sources of electrical noise.')
    logger.debug(bt)
    logger.debug(f'{crccomputed:0>4X}')
    return

  t = bt.decode('ASCII').splitlines()
  tts = time.time()
  logger.debug("Received telegram")
  #logger.debug(t)
  m = decodetelegram(t, tts)

  #TODO publish once and listen when homeassistant sends a discovery request and then publish again, instead of using retain.
  if not discovery_published and 'eqid' in m and 'geqid' in m:
    publish_discovery(m['eqid'], m['geqid'])

  # Update stats
  stats.updatestats(m)

  # Write data
  data.writedata(m, stats, t)
  writetoinflux(m, stats)
  publishdata(m, stats, t)
  if ha_publisher is not None and 'eqid' in m:
    ha_publisher.accumulate(m, stats)
  logger.debug("Written and published data")


if __name__ == '__main__':
  scriptname = os.path.splitext(os.path.basename(__file__))[0]
  parser = argparse.ArgumentParser()
  parser.add_argument('-s', '--serialport', help="Override serial port to use")
  parser.add_argument('-d', '--dryrun',     help="Dry run (do not write to InfluxDB or MQTT)", action='store_true')
  parser.add_argument('-c', '--config',     help="Path to config file (default: "+configfile+")")
  parser.add_argument('-L', '--loglevel',   help="Logging level := { CRITICAL | ERROR | WARNING | INFO | DEBUG } (default: INFO)")
  args = parser.parse_args()

  logger = logging.getLogger()

  own_loglevel = getattr(logging, (args.loglevel or 'INFO').upper(), None)
  if own_loglevel is None:
    print("Unknown log level")
    exit(1)

  logger.setLevel(own_loglevel)
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

  ch = logging.StreamHandler()
  ch.setLevel(own_loglevel)
  ch.setFormatter(formatter)
  logger.addHandler(ch)

  logger.info(f"*** {scriptname} starting... ***")

  if not args.config is None:
    configfile = args.config

  with open(configfile) as cfg:
    config = yaml.load(cfg, Loader=yaml.FullLoader)

  if not args.serialport is None:
    config['serialport'] = args.serialport

  #tstart = re.compile('^\/')
  tcode  = re.compile('^([0-9]-[0-9]:[0-9]{1,2}.[0-9]{1,2}.[0-9]{1,2})\((.*)\)')

  #TODO: incorporate smart meter version
  # get ourselves a serial port
  if float(config['dsmrversion']) > 2.2:
    logger.info(f"Using serial port: {config['serialport']} at 115k2 8N1")
    p1conn = serial.Serial(
      baudrate=115200,
      bytesize=serial.EIGHTBITS,
      parity=serial.PARITY_NONE,
      stopbits=serial.STOPBITS_ONE,
      timeout=1.0,
      exclusive=True
    )
  else:
    logger.info(f"Using serial port: {config['serialport']} at 9k6 7E1")
    p1conn = serial.Serial(
      baudrate=9600,
      bytesize=serial.SEVENBITS,
      parity=serial.PARITY_EVEN,
      stopbits=serial.STOPBITS_ONE,
      timeout=1.0,
      exclusive=True
    )

  p1conn.port = config['serialport']
  try:
    p1conn.open()
  except serial.serialutil.SerialException as e:
    logger.critical(e)
    exit(1)

  data         = cl_data()
  stats        = cl_stats()
  ha_publisher = cl_ha_publisher() if str(config.get('dsmrversion', '')) == '5' else None

  #TODO put influx and mqtt in a class as well.
  influxclient = InfluxDBClient(
    host=config['influxdb']['host'],
    port=config['influxdb']['port'],
    username=config['influxdb']['username'],
    password=config['influxdb']['password'],
    database=config['influxdb']['database']
  )

  mqttclient = mqtt.Client(scriptname)
  mqttclient.on_connect = on_connect
  mqttclient.connect(config['mqtt']['host'])
  mqttclient.loop_start()

  # Set some signal handlers
  signal.signal(signal.SIGHUP, reloadconfig)
  signal.signal(signal.SIGTERM, shutdown)
  #signal.signal(signal.SIGTSTP, suspend)

  logger.info(f"*** {scriptname} running. ***")
  # DSMR sends a 'Telegram' every 1 or 10 second(s) with the current meter reading.
  # let serial.readlines() do the magic and use the remaining time to process the telegram
  try:
    while True:
      process_telegrams()
  except KeyboardInterrupt:
    pass

  logger.info(f"*** {scriptname} stopping. ***")
  stats.writestate()
  data.close()

