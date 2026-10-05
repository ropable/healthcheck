import os
import logging.config
from zoneinfo import ZoneInfo

HOME_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HEALTHCHECK_DATA_DIR = os.environ.get("HEALTHCHECK_DATA_DIR",os.path.join(HOME_DIR,"data_dir"))
if not os.path.exists(HEALTHCHECK_DATA_DIR):
    os.makedirs(HEALTHCHECK_DATA_DIR)

DEBUG = os.environ.get("DEBUG","false").lower() == "true"

TZ = ZoneInfo(os.environ.get("TZ", "Australia/Perth"))  

try:
    HEARTBEAT = int(os.environ.get("HEARTBEAT"),10)
    if HEARTBEAT <=0 :
        HEARTBEAT = 10
except:
    HEARTBEAT = 10

try:
    STATUSSTREAM_HEARTBEAT = int(os.environ.get("STATUSSTREAM_HEARTBEAT"),8)
    if STATUSSTREAM_HEARTBEAT < 0 :
        STATUSSTREAM_HEARTBEAT = 8
except:
    STATUSSTREAM_HEARTBEAT = 8

HEALTHCHECKSERVICE_TIMEOUT = int(os.environ.get("HEALTHCHECKSERVICE_TIMEOUT",5000)) #milliseconds
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT",2000)) #milliseconds
HEALTHCHECKSERVICE_RETRY = int(os.environ.get("HEALTHCHECKSERVICE_RETRY",3)) #retry times
HEALTHCHECKSERVICE_RETRY_INTERVAL = int(os.environ.get("HEALTHCHECKSERVICE_RETRY_INTERVAL",100)) / 1000.0#retry interval, configured as milliseconds, but convert it into seconds

NEXTCHECK_TIMEOUT_DELAY = int(os.environ.get("NEXTCHECK_TIMEOUT_DELAY",500)) #configured in milliseconds
NEXTCHECK_CHECKINTERVAL = int(os.environ.get("NEXTCHECK_CHECKINTERVAL",10)) * 1000 #configured in seconds, tranform it to milliseconds

BLOCK_TIMEOUT = int(os.environ.get("BLOCK_TIMEOUT",5)) # in seconds, 
SOCKET_ATTEMPTS = int(os.environ.get("SOCKET_ATTEMPTS",3))
HEALTHCHECKSERVER_HOST = os.environ.get("HEALTHCHECKSERVER_HOST","localhost")
HEALTHCHECKSERVER_PORT = int(os.environ.get("HEALTHCHECKSERVER_PORT",9080))

HEALTHCHECK_CONFIGFILE = os.path.join(HEALTHCHECK_DATA_DIR,os.environ.get("HEALTHCHECK_CONFIGFILE","healthcheck.json"))

HEALTHCHECK_CONDITION_VERBOSE = os.environ.get("HEALTHCHECK_CONDITION_VERBOSE","false").lower() == "true"

try:
    HEALTHSTATUS_PAGESIZE = int(os.environ.get("HEALTHSTATUS_PAGESIZE",100))
    #if HEALTHSTATUS_PAGESIZE < 100:
    #    HEALTHSTATUS_PAGESIZE = 100
except :
    HEALTHSTATUS_PAGESIZE = 100
HEALTHSTATUS_BUFFER = int(os.environ.get("HEALTHSTATUS_BUFFER",1000))


EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME = HEALTHCHECK_DATA_DIR,os.environ.get("EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME",3600) #in seconds
try:
    EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME = int(EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME)
    if EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME <= 0:
        EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME = 3600
except:
    EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME = 3600

AUTH2_URL = os.environ.get("AUTH2_URL","auth2.dbca.wa.gov.au")
AUTH2_USER = os.environ.get("AUTH2_USER")
AUTH2_PASSWORD = os.environ.get("AUTH2_PASSWORD")
AUTH2_TIMEOUT = float(os.environ.get("AUTH2_TIMEOUT",1))
AUTH2_SSLVERIFY = os.environ.get("AUTH2_SSLVERIFY","true").lower() == "true"
AUTH2_PERMCACHE_TIMEOUT = int(os.environ.get("AUTH2_PERMCACHE_TIMEOUT",300))  # in seconds

HEALTHCHECK_PUBLISH_HISTORIES = int(os.environ.get("HEALTHCHECK_PUBLISH_HISTORIES",100))

ASYNCIO_EVENTS = int(os.environ.get("ASYNCIO_EVENTS",20))

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG' if DEBUG else "INFO",
            'formatter': 'console',
            'class': 'logging.StreamHandler',
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False
        },
        'healthcheck': {
            'handlers': ['console'],
            'level': 'DEBUG' if DEBUG else "INFO",
            'propagate': False
        }
    }
})

