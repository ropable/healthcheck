import sys
import inspect
import re
import hashlib
import shutil
import traceback
import asyncio
import math
import json
import os
import time
import logging
import httpx
import urllib.parse
#import urllib3
from datetime import datetime,timedelta
from collections import UserDict,OrderedDict

from . import checks
from . import settings

from . import exceptions
from . import settings
from . import utils
from . import serializers
from . import shutdown
from .locks import FileLock

logger = logging.getLogger("healthcheck.healthcheck")

#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
PRTGDATA_NOT_ENABLED = "Disabled"

RESERVED_PRTG_CHANNELS = {
    "__status__": {
        "unit": "Custom",
        "customunit": "status",
        "float": 1,
        "value": 0
    }
    ,
    "__processtime__": {
        "unit": "Custom",
        "customunit": "processtime",
        "float": 1,
        "value": 0
    },
    "__attempts__": {
        "unit": "Custom",
        "customunit": "count",
        "float": 1,
        "value": 0
    }
}
RESERVED_PRTG_CHANNELNAME = {
    "__status__": lambda service:"{} Status".format(service["name"]),
    "__processtime__": lambda service: "{} Processtime".format(service["name"]),
    "__attempts__": lambda service: "{} Attempts".format(service["name"])
}

class BaseServiceHealthCheckTask(object):
    def __init__(self,servicehealthcheck):
        self.servicehealthcheck = servicehealthcheck

    async def post_healthcheck(self,healthstatus):
        print(str(healthstatus))
        logger.debug("Health Check({0}.{1}) : {4}{6}  Running from {2} to {3}. {5}".format(
            healthstatus[0][0],
            healthstatus[0][1],
            healthstatus[1][1][0].strftime("%Y-%m-%d %H:%M:%S.%f"),
            healthstatus[1][1][1].strftime("%Y-%m-%d %H:%M:%S.%f"),
            healthstatus[1][1][2],
            healthstatus[1][1][3],
            "(Details Avaiable)" if healthstatus[1][1][4] else "(Detail Not Available)"
        ))
        pass

    async def run(self):
        attempts = 0
        endtime = None
        processtime = 0
        res = None
        healthstatus = None
        nextchecktime = self.servicehealthcheck.healthstatus_nextchecktime
        while not healthstatus:
            attempts += 1
            starttime = utils.now()
            if self.servicehealthcheck.url:
                try:
                    try:
                        res = None
                        data = None
                        #logger.debug("{} : Start to run the healthcheck task({})".format(self.servicehealthcheck,self.__class__.__name__))
                        async with httpx.AsyncClient(auth=self.servicehealthcheck.auth,timeout=self.servicehealthcheck.request_timeout,verify=self.servicehealthcheck.sslverify,headers=self.servicehealthcheck.headers) as client:
                            if self.servicehealthcheck.method == "GET":
                                func = client.get
                            elif self.servicehealthcheck.method == "POST":
                                func = client.post
                                data = self.servicehealthcheck.formdata
                            elif self.servicehealthcheck.method == "PUT":
                                func = client.put
                                data = self.servicehealthcheck.formdata
                            elif self.servicehealthcheck.method == "DELETE":
                                func = client.delete
                            else:
                                #Not support
                                raise Exception("Http method({}) Not Support".format(self.method))
                            if data:
                                res = await func(self.servicehealthcheck.url,data=data)
                            else:
                                res = await func(self.servicehealthcheck.url)
                    finally:
                        endtime = utils.now()
                        processtime = (endtime - starttime).total_seconds() * 1000
        
                    healthstatus = HealthCheck.check_response(self.servicehealthcheck,res,attempts,processtime)
                except (httpx.ConnectTimeout,httpx.NetworkError,httpx.ProxyError) as ex:
                    if attempts >= self.servicehealthcheck.retry:
                        healthstatus = HealthCheck.populate_healthstatus(self.servicehealthcheck,"red","httpx.{1}: {2}. Attempts={0}".format(attempts,ex.__class__.__name__,str(ex)),attempts,processtime)
                    else:
                        if self.servicehealthcheck.retry_interval:
                            await asyncio.sleep(self.servicehealthcheck.retry_interval)
                        continue
                except (httpx.TimeoutException,httpx.StreamError) as ex:
                    if attempts >= self.servicehealthcheck.retry:
                        healthstatus = HealthCheck.populate_healthstatus(self.servicehealthcheck,"red","httpx.{1}: {2}. Attempts={0}".format(attempts,ex.__class__.__name__,str(ex)),attempts,processtime)
                    elif nextchecktime and (nextchecktime - endtime).total_seconds() >= self.servicehealthcheck.total_retry_processtime:
                        #still have time to try again
                        if self.servicehealthcheck.retry_interval:
                            await asyncio.sleep(self.servicehealthcheck.retry_interval)
                        continue
                    else:
                        healthstatus = HealthCheck.populate_healthstatus(self.servicehealthcheck,"red","httpx.{1}: {2}. No time to try again. Attempts={0}".format(attempts,ex.__class__.__name__,str(ex)),attempts,processtime)
                except Exception as ex:
                    if ex.__class__.__module__ == "builtins":
                        healthstatus = HealthCheck.populate_healthstatus(self.servicehealthcheck,"error","{1}: {2}. Attempts={0}".format(attempts,ex.__class__.__name__,str(ex)),attempts,processtime)
                    else:
                        healthstatus = HealthCheck.populate_healthstatus(self.servicehealthcheck,"error","{1}.{2}: {3}. Attempts={0}".format(attempts,ex.__class__.__module__,ex.__class__.__name__,str(ex)),attempts,processtime)
            else:
                healthstatus = ["green","OK",None]
                endtime = utils.now()

        healthstatus.insert(0,starttime)
        healthstatus.insert(1,endtime)
        
        healthstatus.append(healthstatus[2] in self.servicehealthcheck.healthdetailpersistent)
        self.servicehealthcheck.healthstatus_healthdata = healthstatus
        self.servicehealthcheck.healthstatus_running = False

        try:
            await self.servicehealthcheck.save_checkingstatus(healthstatus,res,attempts)
        except Exception as ex:
            traceback.print_exc()
            logger.error("Failed to save the healthcheck status details({2}) of service({0}.{1}). {3}: {4}".format(self.servicehealthcheck.sectionid,self.servicehealthcheck.serviceid,healthstatus,ex.__class__.__name__,str(ex)))

        try:
            if inspect.iscoroutinefunction(self.post_healthcheck):
                await self.post_healthcheck([[self.servicehealthcheck.sectionid,self.servicehealthcheck.serviceid],[self.servicehealthcheck.healthstatus_nextchecktime,healthstatus]])
            else:
                self.post_healthcheck([[self.servicehealthcheck.sectionid,self.servicehealthcheck.serviceid],[self.servicehealthcheck.healthstatus_nextchecktime,healthstatus]])
        except Exception as ex:
            logger.error("Failed to call 'post_healthcheck'({2}) of service({0}.{1}). {3}: {4}".format(self.servicehealthcheck.sectionid,self.servicehealthcheck.serviceid,healthstatus,ex.__class__.__name__,str(ex)))


class SectionHealthCheck(UserDict):

    @property
    def sectionid(self):
        return self["id"]

    @property
    def healthcheckservices(self):
        return self["services"].values()

    @property
    def prtgenabled(self):
        return any(s.prtgenabled for s in self.healthcheckservices)

    @property
    def enabled(self):
        return self["enabled"]

class HealthCheckStatus(object):
    @staticmethod
    def serialize(data):
        return json.dumps(data,cls=serializers.JSONFormater)

    @staticmethod
    def deserialize(data):
        """
        data:a json string with pattern: [check start,check end,health status, msg,prtgdata, persistent]
        """
        if not data:
            return None
        data = data.strip()
        if not data:
            return None
        try:
            checkstatus = json.loads(data)
            checkstatus[0] = utils.parse_datetime(checkstatus[0])
            checkstatus[1] = utils.parse_datetime(checkstatus[1])
        except Exception as ex:
            raise Exception("The healthcheck status({}) is corrupted".format(data))

        return checkstatus

class HealthCheckPage(object):
    _last_greenhealthcheck = None
    _last_yellowhealthcheck = None
    _last_greenyellowhealthcheck = None
    _last_redhealthcheck = None
    _last_errorhealthcheck = None

    def __init__(self,healthcheckpages,starttime,filepath):
        self._healthcheckpages = healthcheckpages
        self._starttime = starttime
        self._filepath = filepath
        self._size = None
        self._last_healthcheck = None

    def __str__(self):
        return "starttime={} , filepath={}".format(self.starttime,self.filepath)
    @property
    def pageid(self):
        return int(self._starttime.timestamp())

    @property
    def starttime(self):
        return self._starttime

    @property
    def filepath(self):
        return self._filepath

    @property
    def last_healthcheck(self):
        if self._size is None:
            self._load()
        return self._last_healthcheck
 
    @property
    def last_greenhealthcheck(self):
        if self._size is None:
            self._load()
        return self._last_greenhealthcheck

    @property
    def last_yellowhealthcheck(self):
        if self._size is None:
            self._load()
        return self._last_yellowhealthcheck

    @property
    def last_greenyellowhealthcheck(self):
        if self._size is None:
            self._load()
        return self._last_greenyellowhealthcheck

    @property
    def last_redhealthcheck(self):
        if self._size is None:
            self._load()
        return self._last_redhealthcheck

    @property
    def last_errorhealthcheck(self):
        if self._size is None:
            self._load()
        return self._last_errorhealthcheck

    def delete(self):
        utils.remove_file(self._filepath)

    def serialize(self):
        return json.dumps([self._starttime.strftime("%Y-%m-%dT%H:%M:%S.%f"),self._filepath[len(self._healthcheckpages.basedir) + 1:]])

    @classmethod
    def deserialize(cls,healthcheckpages,data):
        try:
            pageindexdata = json.loads(data)
            pageindexdata[0] = utils.parse_datetime(pageindexdata[0])
            pageindexdata[1] = os.path.join(healthcheckpages.basedir,pageindexdata[1])
            return cls(healthcheckpages,*pageindexdata)
        except Exception as ex:
            raise Exception("The page index data({}) is corrupted".format(data))

    @property
    def size(self):
        if self._size is None:
            self._load()
        return self._size

    def detailfile(self,starttime):
        return self._healthcheckpages.detailfile(starttime)

    def _load(self):
        if self._size is not None:
            return

        if not os.path.exists(self._filepath):
            self._size = 0
            self._last_healthcheck = None
            folder = os.path.dirname(self._filepath)
            utils.makedir(folder)
        elif not os.path.isfile(self._filepath):
            raise Exception("The file path({}) is not a file".format(self._filepath))
        else:
            size = 0
            with open(self._filepath,'r') as f:
                while True:
                    data = f.readline()
                    if data == "":
                        break
                    size += 1
                    self._last_healthcheck = HealthCheckStatus.deserialize(data)
                    if self._last_healthcheck[2] == "green":
                        self._last_greenhealthcheck = self._last_healthcheck
                        self._last_greenyellowhealthcheck = self._last_healthcheck
                    elif self._last_healthcheck[2] == "yellow":
                        self._last_yellowhealthcheck = self._last_healthcheck
                        self._last_greenyellowhealthcheck = self._last_healthcheck
                    elif self._last_healthcheck[2] == "red":
                        self._last_redhealthcheck = self._last_healthcheck
                    else:
                        self._last_errorhealthcheck = self._last_healthcheck

            self._size = size
        

    def save(self,healthcheckstatus):
        """
        Return True if write; Return False if the page is already full and can't write anymore.

        """
        if self._size is None:
            self._load()
        if self._size >= settings.HEALTHSTATUS_PAGESIZE:
            return False
        data = HealthCheckStatus.serialize(healthcheckstatus)
        with open(self._filepath,'ab') as f:
            if self._size > 0:
                f.write(b"\n")
            f.write(data.encode())
            self._size += 1

        self._last_healthcheck = healthcheckstatus

        return True

    def pageitems(self):
        """
        used by history page 
        Ignore the corrupted data
        Return a generator for healthcheck items in this page

        """
        if self._size is None:
            self._load()
        if self._size  == 0:
            return
        with open(self._filepath,'r') as f:
            while True:
                data = f.readline()
                if data == "":
                    break
                else:
                    try:
                        yield HealthCheckStatus.deserialize(data)
                    except Exception as ex:
                        logger.error("The data({0}) in file({0}) is corrupted.".format(self._filepath,data))
                        continue

    def reversed_pageitems(self):
        data = [d for d in self.pageitems()]
        return reversed(data)

class LastHealthCheck(HealthCheckPage):
    def __init__(self,healthcheckpages,path):
        super().__init__(healthcheckpages,None,path)

    def serialize(self):
        raise Exception("Not Support")

    @classmethod
    def deserialize(cls,data):
        raise Exception("Not Support")

    def save(self,healthcheckstatus):
        """
        Return True if write; Return False if the page is already full and can't write anymore.

        """
        if self._size is None:
            self._load()
        data = HealthCheckStatus.serialize(healthcheckstatus)
        with open(self._filepath,'wb') as f:
            f.write(data.encode())
        self._size += 1

        self._last_healthcheck = healthcheckstatus

        return True

class LastHealthCheckInMemory(LastHealthCheck):

    def detailfile(self,starttime):
        return ""

    def save(self,healthcheckstatus):
        """
        The data is in memeory, no need to save
        Return True if write; Return False if the page is already full and can't write anymore.
        """
        self._size = 1

        self._last_healthcheck = healthcheckstatus

        return True

    def _load(self):
        """
        Never persistent the data to file system
        no need to load the data

        """
        self._size = 0

class BasicHealthCheckPages(object):
    """
    pages: a list of page data([startdatetime,page file])
    """
    _instances = None
    _pageindexfilename = None
    _pagefilename = None
    _pagefolder = None
    def __init__(self,servicehealthcheck):
        self._servicehealthcheck = servicehealthcheck
        self._pages = None #from earlist to latest
        self._filesize = None
        self.next_management_time = None
        self._historyexpire = 0
        self.historyenabled = False


    @classmethod
    def get_instance(cls,servicehealthcheck):
        configdata = cls._instances.get(servicehealthcheck.healthcheck.configfile)
        if not configdata:
            configdata = {}
            cls._instances[servicehealthcheck.healthcheck.configfile] = configdata

        sectiondata = configdata.get(servicehealthcheck.sectionid)
        if not sectiondata:
            obj = cls(servicehealthcheck)
            cls._instances[servicehealthcheck.sectionid] = {servicehealthcheck.serviceid:obj}
        else:
            obj = sectiondata.get(servicehealthcheck.serviceid)
            if not obj:
                obj = cls(servicehealthcheck)
                sectiondata[servicehealthcheck.serviceid] = obj
            else:
                obj.servicehealthcheck = servicehealthcheck

        return obj

    @property
    def servicehealthcheck(self):
        return self._servicehealthcheck

    @servicehealthcheck.setter
    def servicehealthcheck(self,servicehealthcheck):
        if self.historyenabled != servicehealthcheck.historyenabled:
            self._pages = None
        self._servicehealthcheck = servicehealthcheck
        self.historyenabled = self._servicehealthcheck.historyenabled

    _basedir = None
    @property
    def basedir(self):
        if not self._basedir:
            folder = os.path.join(settings.HEALTHCHECK_DATA_DIR,os.path.splitext(os.path.basename(self._servicehealthcheck.healthcheck.configfile))[0],self._servicehealthcheck.sectionid,self._servicehealthcheck.serviceid)
            utils.makedir(folder)
            self._basedir = folder

        return self._basedir

    _pageindexfile = None
    @property
    def pageindexfile(self):
        if not self.historyenabled:
            raise Exception("{}: History health check is disabled".format(self._servicehealthcheck))

        if self._pageindexfile is None:
            self._pageindexfile = os.path.join(self.basedir,self._pageindexfilename)

        return self._pageindexfile

    def pagedir(self,starttime):
        return os.path.join(self.basedir,self._pagefolder)

    def pagefile(self,starttime):
        return os.path.join(self.pagedir(starttime),self._pagefilename.format(starttime.strftime("%Y%m%dT%H%M%S")))

    @property
    def detailsdir(self):
        return  os.path.join(self.basedir,"details")

    def detaildir(self,starttime):
        folder =  os.path.join(self.detailsdir,starttime.strftime("%Y%m%d"))
        utils.makedir(folder)
        return folder

    def detailfile(self,starttime):
        return os.path.join(self.detaildir(starttime),"{}.json".format(starttime.strftime("%Y%m%dT%H%M%S")))

    @property
    def last_healthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            return self._pages[-1].last_healthcheck
        else:
            return None

    def _load(self):
        pages = []
        if not os.path.exists(self.pageindexfile):
            folder = os.path.dirname(self.pageindexfile)
            utils.makedir(folder)
        elif not os.path.isfile(self.pageindexfile):
            raise Exception("The file path({}) is not a file".format(self.pageindexfile))
        else:
            with open(self.pageindexfile,'r') as f:
                while True:
                    data = f.readline()
                    if data == "":
                        break
                    data = data.strip()
                    if not data:
                        continue
                    try:
                        pages.append(HealthCheckPage.deserialize(self,data))
                    except Exception as ex:
                        logger.error("The page data({1}) in file({0}) is corrupted".format(self.pageindexfile,data))

        self._pages = pages
        if os.path.exists(self.pageindexfile):
            self._filesize = os.path.getsize(self.pageindexfile)
        else:
            self._filesize = 0

    def reset(self):
        """
        Reset the pages to reload it
        """
        self._filesize = None
        self._pages = None

    def get_pages(self):
        """
        Called by web app; should reload if if it was changed by healthcheck server
        """
        if self._pages is None or self._filesize is None or (os.path.exists(self.pageindexfile) and self._filesize != os.path.getsize(self.pageindexfile)):
            self._load()
        return self._pages

    def save_healthcheckstatus(self,healthcheckstatus):
        """
        Return True if expired pages have been cleaned; otherwise return False
        """
        if self._pages is None:
            self._load()

        try:
            cleaned = False
            if self._pages:
                if self._pages[-1].save(healthcheckstatus):
                    return False

            newpage = HealthCheckPage(self,healthcheckstatus[0],self.pagefile(healthcheckstatus[0]))

            with open(self.pageindexfile,'ab') as f:
                if self._pages:
                    f.write(b"\n")
                f.write(newpage.serialize().encode())
            self._pages.append(newpage)

            newpage.save(healthcheckstatus)
        except FileNotFoundError as ex:
            #file doesn't exist. maybe delete from disk directly.
            #reload the pages and do it again
            self.reset()
            self.save_healthcheckstatus(healthcheckstatus)
        finally:
            cleaned = self.managepages()

        return cleaned


    def managepages(self):
        """
        Return True if expired pages have been cleaned; otherwise return False
        """
        if not self.historyenabled:
            #no need to manage
            return  False

        now = utils.now()
        if self.next_management_time and now < self.next_management_time:
            manage_history = False
        else :
            manage_history = True
        if self.next_management_time:
            self.next_management_time += timedelta(days=1)
        else:
            self.next_management_time = datetime(now.year,now.month,now.day,tzinfo=settings.TZ) + timedelta(days=1)

        if manage_history:
            ealiest_nonexpiretime = datetime(now.year,now.month,now.day,tzinfo=settings.TZ)
            if self._historyexpire > 1:
                ealiest_nonexpiretime -= timedelta(days=self._historyexpire - 1)
            #find the index of the last expired data
            index_of_latest_expiredata = -1
            for i in range(len(self._pages)):
                if self._pages[i].starttime < ealiest_nonexpiretime:
                    #this page contains some expire data, but also can contain non-expired data.
                    continue
                else:
                    #This is the first page whose data is not expired.
                    #The previous page must contain expired data, and maybe contain non-expired data
                    #Preserve the previous page too
                    index_of_latest_expiredata = i - 2
                    break

            if index_of_latest_expiredata >= 0:
                #remove expired data from memory
                for i in range(index_of_latest_expiredata,-1,-1):
                    self._pages[i].delete()
                    del self._pages[i]
                #save to file
                with open(self.pageindexfile,'wb') as f:
                    for i in range(len(self._pages)):
                        if i > 0:
                            f.write(b"\n")
                        f.write(self._pages[i].serialize().encode())
            return True
        else:
            return False

class HealthCheckPages(BasicHealthCheckPages):
    """
    pages: a list of page data([startdatetime,page file])
    """
    _instances = {}
    _pageindexfilename = "pageindex.json"
    _pagefilename = "page_{}.json"
    _pagefolder = "pages"
    def __init__(self,servicehealthcheck):
        super().__init__(servicehealthcheck)
        self.historyenabled = self._servicehealthcheck.historyenabled
        self._lock = FileLock(os.path.join(self.basedir,".lock"))
        self._historyexpire = self._servicehealthcheck.historyexpire
        self._errorpages = HealthCheckErrorPages.get_instance(servicehealthcheck) if servicehealthcheck.errorhistoryenabled else None

    @property
    def errorpages(self):
        return self._errorpages

    @property
    def last_greenhealthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            for i in range(len(self._pages) - 1,-1,-1):
                data = self._pages[i].last_greenhealthcheck
                if data:
                    return data
            return None
        else:
            return None

    @property
    def last_yellowhealthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            for i in range(len(self._pages) - 1,-1,-1):
                data = self._pages[i].last_yellowhealthcheck
                if data:
                    return data
            return None
        else:
            return None

    @property
    def last_greenyellowhealthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        Return the last green or yellow healthcheck whichever is the latest one.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            for i in range(len(self._pages) - 1,-1,-1):
                data = self._pages[i].last_greenyellowhealthcheck
                if data:
                    return data

            return None
        else:
            return None
    @property
    def last_redhealthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            for i in range(len(self._pages) - 1,-1,-1):
                data = self._pages[i].last_redhealthcheck
                if data:
                    return data
            return None
        else:
            return None

    @property
    def last_errorhealthcheck(self):
        """
        Called by healthcheck server
        because _pages are loaded and catched in memory and only healthcheck server can change this file, no need to check whether the file was changed by other process after loading.
        """
        if self._pages is None:
            self._load()
        if self._pages:
            for i in range(len(self._pages) - 1,-1,-1):
                data = self._pages[i].last_errorhealthcheck
                if data:
                    return data
            return None
        else:
            return None

    def pagefile(self,starttime):
        if self.historyenabled:
            return super().pagefile(starttime)
        else:
            return os.path.join(self.basedir,"latesthealthcheck.json")

    def detailfile(self,starttime):
        if not self._servicehealthcheck.url:
            #is not a real healthcheck,for example: heartbeat
            raise Exception("Not Support")
        elif not self.historyenabled:
            return os.path.join(self.basedir,"lasthealthcheckdetails.json")
        else:
            return super().detailfile(starttime)
        
        return os.path.join(self.basedir,"{}.json".format(starttime.strftime("%Y%m%dT%H%M%S")))

    def _load(self):
        if not self._servicehealthcheck.url:
            #is not a real healthcheck,for example: heartbeat
            self._pages = [LastHealthCheckInMemory(self,self.pagefile(None))]
        elif not self.historyenabled:
            self._pages = [LastHealthCheck(self,self.pagefile(None))]
        else:
            super()._load()

    def save(self,healthcheckstatus,details=None):
        cleaned = False
        with self._lock:
            try:
                cleaned = super().save_healthcheckstatus(healthcheckstatus)
            finally:
                if details:
                    with open(self._pages[-1].detailfile(healthcheckstatus[0]),'w') as f:
                        f.write(json.dumps(details,cls=serializers.JSONFormater))

            if self._errorpages and healthcheckstatus[2] != "green" and healthcheckstatus[2] in self._servicehealthcheck.healthdetailpersistent:
                cleaned = cleaned or self._errorpages.save_healthcheckstatus(healthcheckstatus)
            
            if cleaned:
                #a history clean action was performed. try to clean the expired details
                #Find the earliest starttime of the details
                starttime = None
                if self._historyexpire > 0:
                    starttime = self._pages[0]._starttime
                if self._errorpages._historyexpire > 0 and self._errorpages._pages:
                    if not starttime:
                        starttime = self._errorpages._pages[0]._starttime
                    elif starttime > self._errorpages._pages[0]._starttime:
                        starttime = self._errorpages._pages[0]._starttime
                if starttime:
                    #find the start time, clean all the details before that day
                    ealiest_nonexpiretime = starttime.strftime("%Y-%m-%d")
                    if os.path.exists(self.detailsdir):
                        for d in os.listdir(self.detailsdir):
                            if d < ealiest_nonexpiretime:
                                utils.deletedir(os.path.join(self.detailsdir,d))

class HealthCheckErrorPages(BasicHealthCheckPages):
    """
    pages: a list of page data([startdatetime,page file])
    """
    _instances = {}
    _pageindexfilename = "errorpageindex.json"
    _pagefilename = "errorpage_{}.json"
    _pagefolder = "errorpages"
    def __init__(self,servicehealthcheck):
        super().__init__(servicehealthcheck)
        self.historyenabled = self._servicehealthcheck.errorhistoryenabled
        self._historyexpire = self._servicehealthcheck.errorhistoryexpire


class ServiceHealthCheck(UserDict):
    selected = False

    def __init__(self,healthcheck,data):
        super().__init__(data)
        self.healthcheck = healthcheck
        self.healthcheckpages = HealthCheckPages.get_instance(self)

    def __str__(self):
        return "{}.{}.{}".format(self.healthcheck,self.sectionid,self.serviceid)

    _last_greenhealthcheck = "__NULL__"
    @property
    def last_greenhealthcheck(self):
        if self._last_greenhealthcheck == "__NULL__": 
            self._last_greenhealthcheck = self.healthcheckpages.last_greenhealthcheck
        return self._last_greenhealthcheck

    @last_greenhealthcheck.setter
    def last_greenhealthcheck(self,val):
        self._last_greenhealthcheck = val

    _last_yellowhealthcheck = "__NULL__"
    @property
    def last_yellowhealthcheck(self):
        if self._last_yellowhealthcheck == "__NULL__": 
            self._last_yellowhealthcheck = self.healthcheckpages.last_yellowhealthcheck
        return self._last_yellowhealthcheck

    @last_yellowhealthcheck.setter
    def last_yellowhealthcheck(self,val):
        self._last_yellowhealthcheck = val

    _last_greenyellowhealthcheck = "__NULL__"
    @property
    def last_greenyellowhealthcheck(self):
        if self._last_greenyellowhealthcheck == "__NULL__": 
            self._last_greenyellowhealthcheck = self.healthcheckpages.last_greenyellowhealthcheck
        return self._last_greenyellowhealthcheck

    @last_greenyellowhealthcheck.setter
    def last_greenyellowhealthcheck(self,val):
        self._last_greenyellowhealthcheck = val

    _last_redhealthcheck = "__NULL__"
    @property
    def last_redhealthcheck(self):
        if self._last_redhealthcheck == "__NULL__": 
            self._last_redhealthcheck = self.healthcheckpages.last_redhealthcheck
        return self._last_redhealthcheck

    @last_redhealthcheck.setter
    def last_redhealthcheck(self,val):
        self._last_redhealthcheck = val

    _last_errorhealthcheck = "__NULL__"
    @property
    def last_errorhealthcheck(self):
        if self._last_errorhealthcheck == "__NULL__": 
            self._last_errorhealthcheck = self.healthcheckpages.last_errorhealthcheck
        return self._last_errorhealthcheck

    @last_errorhealthcheck.setter
    def last_errorhealthcheck(self,val):
        self._last_errorhealthcheck = val

    @property
    def sectionname(self):
        return self['section']["name"]

    @property
    def servicename(self):
        return self['name']

    @property
    def sectionid(self):
        return self['section']["id"]

    @property
    def serviceid(self):
        return self['id']

    @property
    def criticalweight(self):
        return self['criticalweight']

    @property
    def prtg(self):
        return self['prtg']

    @property
    def enabled(self):
        return self["enabled"]

    @property
    def prtgchannels(self):
        """
        A iterator of (channelid,[channelconfig,get_prtgdata,computed_columns])
        """
        return self['prtg'].items() if self['prtg'] else []

    @property
    def offset(self):
        return self['offset']

    @property
    def method(self):
        return self['method']

    @property
    def url(self):
        return self['location']

    @property
    def headers(self):
        return self["headers"]

    _auth = None
    @property
    def auth(self):
        if not self._auth:
            if self.get("user"):
                self._auth = (self["user"],self.get("password"))
            else:
                return None

        return self._auth

    @property
    def user(self):
        return self.get("user")

    @property
    def request_timeout(self):
        return self["request_timeout"]

    @property
    def total_retry_processtime(self):
        return self["total_retry_processtime"]

    @property
    def timeout(self):
        return self["timeout"]

    @property
    def retry(self):
        return self["retry"]

    @property
    def retry_interval(self):
        return self["retry_interval"]

    @property
    def sslverify(self):
        return self["sslverify"]

    @property
    def interval(self):
        return self["interval"]

    @property
    def formdata(self):
        """
        Return the request data
        """
        return self.get("data")

    @property
    def healthstatus(self):
        """
        server side: [next checktime,[starttime,endtime,health status,health status message,prtg data,health checking persistent?],running?] 
        app side: [next checktime,[starttime,endtime,health status,health status message,prtg data,health checking persistent?]] 
        """
        return self.get("healthstatus")

    @healthstatus.setter
    def healthstatus(self,val):
        self["healthstatus"] = val

    @property
    def healthstatus_running(self):
        """
        Only available in server side
        """
        status = self.get("healthstatus")
        return status[2] if status else False

    @healthstatus_running.setter
    def healthstatus_running(self,val):
        """
        Only available in server side
        """
        self["healthstatus"][2] = val

    @property
    def healthstatus_healthdata(self):
        status = self.get("healthstatus")
        return status[1] if status else None

    @healthstatus_healthdata.setter
    def healthstatus_healthdata(self,val):
        if val[2] == "green":
            self.last_greenhealthcheck = val
            self.last_greenyellowhealthcheck = val
        elif val[2] == "yellow":
            self.last_yellowhealthcheck = val
            self.last_greenyellowhealthcheck = val
        elif val[2] == "red":
            self.last_redhealthcheck = val
        else:
            self.last_errorhealthcheck = val
        self["healthstatus"][1] = val

    @property
    def healthstatus_nextchecktime(self):
        status = self.get("healthstatus")
        return status[0] if status else None

    @healthstatus_nextchecktime.setter
    def healthstatus_nextchecktime(self,val):
        self["healthstatus"][0] = val

    @property
    def healthstatus_nextcheck(self):
        status = self.get("healthstatus")
        return status[0].strftime("%Y-%m-%dT%H:%M:%S") if status else ""

    @property
    def healthstatus_name(self):
        status = self.get("healthstatus")
        return status[1][2] if status and status[1] else ""

    @property
    def healthstatus_info(self):
        status = self.get("healthstatus")
        return status[1][3] if status and status[1] else ""

    @property
    def healthstatus_prtgdata(self):
        status = self.get("healthstatus")
        return status[1][4] if status and status[1] and len(status[1]) >= 6 else None

    @property
    def healthstatus_persistent(self):
        status = self.get("healthstatus")
        return status[1][-1] if status and status[1] else False

    @property
    def healthstatus_checkstart(self):
        status = self.get("healthstatus")
        return status[1][0].strftime("%Y-%m-%dT%H:%M:%S.%f") if status and status[1] else ""

    @property
    def healthstatus_checkend(self):
        status = self.get("healthstatus")
        return status[1][1].strftime("%Y-%m-%dT%H:%M:%S.%f") if status and status[1] else ""

    @property
    def historyexpire(self):
        return self["historyexpire"]

    @property
    def historyenabled(self):
        return self.historyexpire > 0

    @property
    def errorhistoryexpire(self):
        return self["errorhistoryexpire"]

    @property
    def errorhistoryenabled(self):
        return self.errorhistoryexpire > 0

    @property
    def healthdetailpersistent(self):
        return self["healthdetailpersistent"]


    @property
    def prtgenabled(self):
        return True if self.prtg else False

    def get_nextchecktime(self,offset,last_checkingtime,now=None,today=None,tomorrow=None,seconds_in_day=None):
        if not now:
            now = utils.now()
            today = datetime(year = now.year,month=now.month,day=now.day,tzinfo=settings.TZ)
            tomorrow = today + timedelta(days=1)
            seconds_in_day = int((now - today).total_seconds())

        nextchecktimeseconds_without_offset = seconds_in_day - (seconds_in_day % self.interval)
        nextchecktime = today + timedelta(seconds=nextchecktimeseconds_without_offset + offset)

        if last_checkingtime and nextchecktime <= last_checkingtime:
            addedseconds = self.interval * math.ceil(((last_checkingtime - nextchecktime).total_seconds() + 1)/self.interval)
            nextchecktimeseconds_without_offset += addedseconds
            nextchecktime_without_offset = today + timedelta(seconds=nextchecktimeseconds_without_offset)

            nextchecktime += timedelta(seconds=addedseconds)
            if nextchecktime_without_offset >= tomorrow:
                nextchecktime_without_offset = tomorrow
                nextchecktimeseconds_without_offset = 0
                nextchecktime = nextchecktime_without_offset + timedelta(seconds=offset)


        checkingtime = self["checkingtime"]
        if not checkingtime:
            return nextchecktime

        index = -1
        for i in range(len(checkingtime)):
            starttime = checkingtime[i][0]
            endtime = checkingtime[i][1]
            if nextchecktimeseconds_without_offset >= starttime and nextchecktimeseconds_without_offset < endtime:
                return nextchecktime
            elif nextchecktimeseconds_without_offset < starttime:
                if starttime % self.interval == 0:
                    return today + timedelta(seconds=starttime + offset)
                else:
                    return today + timedelta(seconds=starttime + self.interval - (starttime % self.interval) + offset)

        #can't find the next check time in the same day, try next day
        if starttime % self.interval == 0:
            return tomorrow + timedelta(seconds=checkingtime[0][0] + offset)
        else:
            return tomorrow + timedelta(seconds=checkingtime[0][0] + self.interval - (checkingtime[0][0] % self.interval) + offset)

    async def save_checkingstatus(self,healthstatus,res,attempts):
        if healthstatus[-1]:
            details = {
                "request": {
                    "url": self.url,
                    "method": self.method,
                    "sslverify" :self.sslverify,
                    "user": self.user

                },
                "healthstatus":healthstatus,
            }
            if attempts > 1:
                details["request"]["attempts"] = attempts
            if self.headers:
                details["request"]["headers"] = self.headers
    
            if self.timeout:
                details["request"]["timeout"] = self.timeout
    
            if self.method in ("POST","PUT"):
                details["request"]["data"] = self.formdata
    
            if res:
                try:
                    content_type = res.headers.get("Content-Type")
                    if "json" in content_type:
                        body = res.json()
                    elif any(key in content_type for key in ("text","html","xml")):
                        body = res.text
                    else:
                        body = "Non text repsonse"
                except Exception as ex:
                    body = "Failed to get body.{}: {}".format(ex.__class__.__name__,str(ex))
                details["response"] = {
                    "status_code" :  res.status_code,
                    "headers": [],
                    "body": body
                }
                for name,val in res.headers.items():
                    if name.lower() in ("connection","server","x-frame-options","x-content-type-options","access-control-allow-methods","content-encoding","accept-ranges","date","via","vary","transfer-encoding"):
                        continue
                    details["response"]["headers"].append([name,val])
        else:
            details = None

        self.healthcheckpages.save(healthstatus,details)

    def load_checkinghistory(self):
        last_healthcheck = self.healthcheckpages.last_healthcheck

        next_checktime = self.get_nextchecktime(self["offset"],last_healthcheck[0] if last_healthcheck else None)
        self.healthstatus = [next_checktime,last_healthcheck,False] 


class JsonStatusMixin(object):
    def get_jsonstatus(self,details=False):
        now = utils.now()
        data = {}
        for section in self.healthchecksections:
            sectiondata = {}
            data[section.sectionid] = sectiondata
            for service in section.healthcheckservices:
                if details:
                    if service.healthstatus_nextchecktime + timedelta(milliseconds=service["timeout"]) < now:
                        sectiondata[service.serviceid] = {
                            'status': "error",
                            'starttime': "",
                            'endtime': "",
                            'message': "The service should be checked at '{}', but it didn't".format(service.healthstatus_nextcheck),
                            'nextcheck':""
                        }
                    else:
                        sectiondata[service.serviceid] = {
                            'status': service.healthstatus_name,
                            'starttime': service.healthstatus_checkstart,
                            'endtime': service.healthstatus_checkend,
                            'message':service.healthstatus_info,
                            'nextcheck':service.healthstatus_nextcheck
                        }
                else:
                    if service.healthstatus_nextchecktime + timedelta(milliseconds=service["timeout"]) < now:
                        #the current healthstatus is outdated
                        sectiondata[service.serviceid] = "error"
                    else:
                        sectiondata[service.serviceid] = service.healthstatus_name
        return data

class PRTGMixin(object):
    def get_prtgdata(self,details=False):
        data = {"error":0,"result":[],"text":"All checks passed"}
        failed_services = []
        warning_services = []
        now = utils.now()
        servicecritical = {}
        for section in self.healthchecksections:
            if not section.enabled:
                continue
            if not section.prtgenabled:
                continue
            for service in section.healthcheckservices:
                if not service.enabled:
                    continue
                if not service.url:
                    continue
                if not service.prtgenabled:
                    continue

                if not service.healthstatus_nextchecktime or service.healthstatus_nextchecktime + timedelta(milliseconds=service["timeout"]) < now:
                    #the current healthstatus is outdated
                    prtgdata = None
                    healthstatus_name = "error"
                else:
                    prtgdata = service.healthstatus_prtgdata
                    healthstatus_name = service.healthstatus_name

                if prtgdata == PRTGDATA_NOT_ENABLED:
                    continue

                if service.prtg :
                    for channelid,prtgconfig in service.prtgchannels:
                        prtgchannel,getdata_map,computed_columns = prtgconfig
                        prtgchannel = dict(prtgchannel)
                        if prtgdata is not None and prtgdata.get(channelid) is not None:
                            prtgchannel["value"] = prtgdata[channelid]

                        for k,v in computed_columns.items():
                            prtgchannel[k] = v(prtgchannel["value"])


                        data["result"].append(prtgchannel)

                if healthstatus_name in ("red","error"):
                    if service.criticalweight:
                        servicecritical[service.criticalweight[0]] = servicecritical.get(service.criticalweight[0],0) + service.criticalweight[1]
                    failed_services.append(service.servicename)
                elif healthstatus_name  == "yellow":
                    warning_services.append(service.servicename)

        if any( (v >= 1) for v in servicecritical.values()):
            data["error"] = 1

        if failed_services or warning_services:
            if failed_services and warning_services:
                data["text"] = "The services({}) are not available; The services({}) aren't healthy.".format(failed_services,warning_services)
            elif failed_services:
                data["text"] = "The services({}) are not available.".format(failed_services)
            else:
                data["text"] = "The services({}) aren't healthy.".format(warning_services)

        return {"prtg":data}

class HealthCheck(PRTGMixin,JsonStatusMixin):
    configfile = None
    _checkingstatus_loaded = False
    ID_RE = re.compile("^[a-zA-Z0-9\\-_]+$")
    def __init__(self,configfile=settings.HEALTHCHECK_CONFIGFILE):
        """
        configs : a json file or list object
        """
        self.config_hashcode = None
        self.sections = None
        self.configfile = configfile
        self._name = "{}({})".format(self.__class__.__name__,os.path.basename(self.configfile))
        self._continuous_check_task = None
        self.load_configs()

    def __str__(self):
        return self._name

    @property
    def title(self):
        return "DBCA Essential Systems Health Check"

    @property
    def healthchecksections(self):
        return self.sections.values() if self.sections else []

    def get_service(self,section,service):
        return self.sections.get(section,{}).get("services",{}).get(service)

    def reload(self):
        """
        Reload the configuration 
        """
        sections = self.sections
        changed = self.load_configs()
        if not changed:
            return False

        now = utils.now()
        today = datetime(year = now.year,month=now.month,day=now.day,tzinfo=settings.TZ)
        tomorrow = today + timedelta(days=1)
        seconds_in_day = int((now - today).total_seconds())
        for section in self.healthchecksections:
            for service in section.healthcheckservices:
                existing_service = sections.get(section.sectionid,{}).get("services",{}).get(service.serviceid)
                next_checktime = today + timedelta(seconds=seconds_in_day - (seconds_in_day % service.interval))
                if existing_service and existing_service.healthstatus:
                    service.healthstatus = existing_service.healthstatus
                    if service.healthstatus[0] < next_checktime:
                        service.healthstatus[0] = next_checktime
                    else:
                        next_checktime += timedelta(seconds=service.interval)
                        if next_checktime  >= tomorrow:
                            service.healthstatus[0] = tomorrow
                        else:
                            service.healthstatus[0] = next_checktime
                else:
                    service.healthstatus = [next_checktime,None,False] 
                if existing_service:
                    service._last_greenhealthcheck = existing_service._last_greenhealthcheck
                    service._last_yellowhealthcheck = existing_service._last_yellowhealthcheck
                    service._last_greenyellowhealthcheck = existing_service._last_greenyellowhealthcheck
                    service._last_redhealthcheck = existing_service._last_redhealthcheck
                    service._last_errorhealthcheck = existing_service._last_errorhealthcheck

        return True


    def load_configs(self,force=False):
        """
        Return True if loaded; False if not changed.

        """
        if not self.configfile:
            raise Exception("The configuration is not loaded from a json file, no need to load")
        if not os.path.exists(self.configfile):
            with open(self.configfile,'w') as f:
                f.write("{}")
        try:
            logger.debug("Begin to load health check configurations({})".format(self.configfile))
            with open(self.configfile,'rb') as f:
                configs = f.read()
            config_hashcode = hashlib.sha256(configs).hexdigest()
            if not force and config_hashcode == self.config_hashcode:
                return False

            self.config_hashcode = config_hashcode
            self.sections,errors = self.init_configs(json.loads(configs.decode()))
            if errors:
                logger.warning("""Found {1} issues in healthcheck configuration file({0})
    {2}
""".format(self.configfile,len(errors),"\n    ".join(errors)))

            return True
        except Exception as ex:
            traceback.print_exc()
            raise Exception("The config file({}) is an invalid jason file. {} : {}".format(self.configfile,ex.__class__.__name__,str(ex)))

    def load_checkingstatus(self):
        logger.debug("Begin to load health checking status")
        for section in self.sections.values():
            for service in section["services"].values():
                service.load_checkinghistory()
        self._checkingstatus_loaded = True


    def init_configs(self,configs):
        #initialize the configs
        now = utils.now()
        today = datetime(year = now.year,month=now.month,day=now.day,tzinfo=settings.TZ)
        tomorrow = today + timedelta(days=1)
        seconds_in_day = int((now - today).total_seconds())
        if not configs:
            return ({},[])
        
        if not isinstance(configs,(list,tuple)):
            raise Exception("Healthcheck configurations should be a list object.{}".format(configs))

        sections = OrderedDict()
        #add a fake service for healthcheck heartbeat
        healthchecksection = {
            "id": "Healthcheck",
            "name": "",
            "interval":settings.HEARTBEAT,
            "healthdetailpersistent":[],
            "historyexpire":0,
            "enabled":True,
            "errorhistoryexpire":0,
            "services":{}
        }
        healthchecksection["services"]["Healthcheck-Heartbeat"] = ServiceHealthCheck(self,{
            "section":healthchecksection,
            "id":"Healthcheck-Heartbeat",
            "name":"Healthcheck Heartbeat",
            "interval":settings.HEARTBEAT,
            "location":"",
            "healthdetailpersistent":[],
            "historyexpire":0,
            "errorhistoryexpire":0,
            "timeout":100,
            "offset":0,
            "prtg":None,
            "enabled":True,
            "checkingtime":None
        })
        sections[healthchecksection["id"]] = SectionHealthCheck(healthchecksection)
       

        errors = []

        sectionindex = 0
        for config in configs:
            sectionindex += 1
            config = SectionHealthCheck(config)
            sectionid = config.get("id")
            if not sectionid:
                errors.append("Section {}: Missing property(id)".format(sectionindex))
                continue
            
            if not config.get("services"):
                errors.append("Section {}({}): No services are configured".format(sectionindex,sectionid))
                continue

            baseurl = config.get("baseurl")
            baseurl = baseurl.strip() if baseurl else None
            if baseurl and baseurl.endswith("/"):
                baseurl = baseurl[:-1]
            if baseurl:
                config["baseurl"] = baseurl

            config["enabled"] = True if config.get("enabled",True) else False

            basecheckingtime = config.get("checkingtime")
            try:
                basecheckingtime = checks.parse_checkingtime(basecheckingtime)
            except Exception as ex:
                errors.append("Section {}({}): The checktime({}) is incorrect.{}".format(sectionindex,sectionid,basecheckingtime,str(ex)))
                basecheckingtime = None

            config["checkingtime"] = basecheckingtime

            try:
                baseoffset = int(config.get("offset") or 0)
            except Exception as ex:
                errors.append("Section {}({}): The offset({}) is incorrect.{}".format(sectionindex,sectionid,config.get("offset"),str(ex)))
                baseoffset = 0

            config["offset"] = baseoffset

            baseheaders = config.get("headers")
            if not baseheaders:
                baseheaders = {}
                config["headers"] = baseheaders

            basequeryparameters = config.get("queryparameters")
            if not basequeryparameters:
                basequeryparameters = {}
                config["queryparameters"] = basequeryparameters
    
            try:
                baseinterval = config.get("interval")
                if baseinterval and not isinstance(baseinterval,int):
                    baseinterval = int(str(baseinterval).strip())
                    config["interval"] = baseinterval
            except Exception as ex:
                errors.append("Section {}({}): The proprety 'interval'({}) is not an integer.".format(sectionindex,sectionid,config.get("interval")))
                continue
    
            try:
                basetimeout = config.get("timeout")
                if basetimeout is None:
                    basetimeout = settings.HEALTHCHECKSERVICE_TIMEOUT
                elif not isinstance(basetimeout,int):
                    basetimeout = int(str(basetimeout).strip())
                config["timeout"] = basetimeout
            except Exception as ex:
                errors.append("Section {}({}): The property 'timeout'({}) is not an integer.".format(sectionindex,sectionid,config.get("timeout")))
                basetimeout = settings.HEALTHCHECKSERVICE_TIMEOUT
                continue

            try:
                baseretry = config.get("retry")
                if baseretry is None:
                    baseretry = settings.HEALTHCHECKSERVICE_RETRY
                elif not isinstance(baseretry,int):
                    baseretry = int(str(baseretry).strip())
                config["retry"] = baseretry
            except Exception as ex:
                errors.append("Section {}({}): The property 'retry'({}) is not an integer.".format(sectionindex,sectionid,config.get("retry")))
                baseretry = settings.HEALTHCHECKSERVICE_RETRY
                continue

            try:
                baseretryinterval = config.get("retry_interval")
                if not baseretryinterval:
                    baseretryinterval = settings.HEALTHCHECKSERVICE_RETRY_INTERVAL
                elif not isinstance(baseretryinterval,int):
                    baseretryinterval = int(str(baseretryinterval).strip()) / 1000
                else:
                    baseretryinterval = baseretryinterval / 1000
                config["retryinterval"] = baseretryinterval
            except Exception as ex:
                errors.append("Section {}({}): The property 'retry_interval'({}) is not an integer.".format(sectionindex,sectionid,config.get("retry_interval")))
                baseretryinterval = settings.HEALTHCHECKSERVICE_RETRY_INTERVAL
                continue

            basemethod = config.get("method")
            if basemethod:
                basemethod = basemethod.upper()
                if basemethod not in ["GET","POST","DELETE","PUT"]:
                    errors.append("Section {}({}): The method({}) doesn't support.".format(sectionindex,sectionid,basemethod))
                    continue
            else:
                basemethod = "GET"
            config["method"] = basemethod

            try:
                basehistoryexpire = config.get("historyexpire")
                if basehistoryexpire is not None:
                    if not isinstance(basehistoryexpire,int):
                        basehistoryexpire = int(str(basehistoryexpire).strip())
                else:
                    basehistoryexpire = 0
    
                if basehistoryexpire < 0:
                    basehistoryexpire = 0
            except Exception as ex:
                errors.append("Section {}({}): The historyexpire({}) is not an integer.{}: {}".format(sectionindex,sectionid,config.get("historyexpire"),ex.__class__.__name__,str(ex)))
                continue
            config["historyexpire"] = basehistoryexpire

            try:
                baseerrorhistoryexpire = config.get("errorhistoryexpire")
                if baseerrorhistoryexpire is not None:
                    if not isinstance(baseerrorhistoryexpire,int):
                        baseerrorhistoryexpire = int(str(baseerrorhistoryexpire).strip())
                else:
                    baseerrorhistoryexpire = basehistoryexpire
    
                if baseerrorhistoryexpire < 0:
                    baseerrorhistoryexpire = basehistoryexpire
            except Exception as ex:
                errors.append("Section {}({}): The errorhistoryexpire({}) is not an integer.{}: {}".format(sectionindex,sectionid,config.get("errorhistoryexpire"),ex.__class__.__name__,str(ex)))
                continue
            config["errorhistoryexpire"] = baseerrorhistoryexpire

            basehealthdetailpersistent = config.get("healthdetailpersistent")
            if basehealthdetailpersistent:
                if isinstance(basehealthdetailpersistent,str) and basehealthdetailpersistent == "__all__":
                    basehealthdetailpersistent = set(["green","yellow","red","error"])
                else:
                    if isinstance(basehealthdetailpersistent,str):
                        basehealthdetailpersistent = [s for s in basehealthdetailpersistent.split(",")]
                    data = set()
                    for s in basehealthdetailpersistent:
                        s = s.strip().lower()
                        if not s:
                            continue
                        if s not in ["green","yellow","red","error"]:
                            errors.append("Section {}({}): The health status({}) in property('healthdetailpersistent') doesn't' support".format(sectionindex,sectionid,s))
                        else:
                            data.add(s)
                    basehealthdetailpersistent = data
            else:
                basehealthdetailpersistent = set(["yellow","red","error"])
            config["healthdetailpersistent"] = basehealthdetailpersistent


            if "sslverify" not in config:
                config["sslverify"] = True
            elif not isinstance(config["sslverify"],bool):
                config["sslverify"] = str(config["sslverify"]).lower() == "true"
    
            if not config.get("name"):
                config["name"] = sectionid

            if not config.get("services"):
                errors.append("Section {}({}): No service are configured".format(sectionindex,sectionid))
                continue

            baseprtgconfig = config.get("prtg")

            if baseprtgconfig:
                if isinstance(baseprtgconfig,list):
                    if len(baseprtgconfig) == 0:
                        baseprtgconfig = None
                else:
                    baseprtgconfig = [baseprtgconfig]
    
                if baseprtgconfig:
                    failed = False
                    #initialize the prtg config
                    #convert list to map
                    configs = {}
                    for prtgconfig in baseprtgconfig:
                        prtgconfig["unit"] = prtgconfig.get("unit") or "Custom"
                        if  prtgconfig["unit"].lower() == "custom":
                            prtgconfig["customunit"] = prtgconfig.get("customunit") or "status"
                        elif "customunit" in prtgconfig:
                            del  prtgconfig["customunit"]

                        prtgconfig["value"] = prtgconfig.get("value") or 0
                        if "id" in prtgconfig:
                            channelid = prtgconfig.pop("id")
                            if not self.ID_RE.search(channelid):
                                errors.append("Section {0}({1}): The channel id({2}) can only contain letters, numbers,'-' and '_'".format(sectionindex,sectionid,channelid))
                                failed = True
                                break

                        else:
                            errors.append("Section {0}({1}): Missing property 'id' in prtg config".format(sectionindex,sectionid))
                            failed = True
                            break

                        configs[channelid] = prtgconfig

                        for k in prtgconfig.keys():
                            v = prtgconfig[k]
                            if not v:
                                continue
                            if not isinstance(v,str):
                                continue
                            v = v.strip()
                            if v.startswith("lambda"):
                                try:
                                    prtgconfig[k] = eval(v)
                                except:
                                    errors.append("Section {0}({1}): the lambda expression({3}) of the prtg data key({2}) is invalid.".format(sectionindex,sectionid,k,v))
                                    failed = True
                                    break

                        if failed:
                            break


                    if failed:
                        continue

                    baseprtgconfig = configs

            #add the reserved prtg data to baseprtgconfig
            if baseprtgconfig:
                for k,v in RESERVED_PRTG_CHANNELS.items():
                    if k in baseprtgconfig:
                        #configured , ignore the RESERVED config
                        continue
                    baseprtgconfig[k] = v
            else:
                baseprtgconfig = RESERVED_PRTG_CHANNELS

            services = OrderedDict()
            serviceindex = 0
            for service in config["services"]:
                serviceindex += 1
                serviceid = service.get("id")
                if not serviceid:
                    errors.append("Service {0}({1}).{2}: Missing property(id)".format(sectionindex,sectionid,serviceindex))
                    continue

                if "user" not in service and config.get("user"):
                    service["user"] = config["user"]
    
                if "password" not in service and config.get("password"):
                    service["password"] = config["password"]
    
                if "sslverify" not in service:
                    service["sslverify"] = config["sslverify"]
                elif not isinstance(service["sslverify"],bool):
                    service["sslverify"] = str(service["sslverify"]).lower() == "true"
    
    
                if not service.get("name"):
                    service["name"] = serviceid

                method = service.get("method")
                if method:
                    method = method.upper()
                    if method not in ["GET","POST","DELETE","PUT"]:
                        errors.append("Service {0}({1}).{2}({3}): The method({}) doesn't support".format(sectionindex,sectionid,serviceindex,serviceid,method))
                        continue
                else:
                    method = basemethod
                service["method"] = method

                service["enabled"] = True if service.get("enabled",True) else False

                try:
                    if "criticalweight" in service:
                        criticalweight = service["criticalweight"] 
                        if ":" in criticalweight:
                            criticalweight = [d.strip() for d in criticalweight.split(":",1)]
                            if not criticalweight[0]:
                                criticalweight[0] = "__default__"
                            criticalweight[1] = float(criticalweight[1])
                        else:
                            criticalweight = ["__default__",float(criticalweight)]

                    else:
                        criticalweight = None
                except:
                    errors.append("Service {0}({1}).{2}: criticalweight({3}) is incorrect.".format(sectionindex,sectionid,serviceindex,criticalweight))
                    criticalweight = None
                service["criticalweight"] = criticalweight


                queryparameters = service.get("queryparameters")
                if not queryparameters:
                    queryparameters = basequeryparameters
                else:
                    for k,v in basequeryparameters.items():
                        if k not in queryparameters:
                            queryparameters[k] = v
                        elif queryparameters[k] is None:
                            del queryparameters[k]
                service["queryparameters"] = queryparameters

                headers = service.get("headers")
                if not headers:
                    headers = baseheaders
                else:
                    for k,v in baseheaders.items():
                        if k not in headers:
                            headers[k] = v
                        elif headers[k] is None:
                            del headers[k]
                service["headers"] = headers if headers else None

                checkingtime = service.get("checkingtime")
                if checkingtime:
                    try:
                        checkingtime = checks.parse_checkingtime(checkingtime)
                    except Exception as ex:
                        errors.append("Service {0}({1}).{2}: The checktime({3}) is incorrect.{4}".format(sectionindex,sectionid,serviceindex,checkingtime,str(ex)))
                        checkingtime = None
                else:
                    checkingtime = basecheckingtime
                service["checkingtime"] = checkingtime

                try:
                    offset = int(service.get("offset") or baseoffset)
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}: The offset({3}) is incorrect.{4}".format(sectionindex,sectionid,serviceindex,service.get("offset"),str(ex)))
                    offset = baseoffset
                service["offset"] = offset

                location = service.get("location")
                location = location.strip() if location else None
                if not location and not baseurl:
                    errors.append("Service {0}({1}).{2}({3}): Missing property(location)".format(sectionindex,sectionid,serviceindex,serviceid))
                    continue
                elif not location:
                    location = baseurl
                elif not location.startswith("http"):
                    if baseurl:
                        if location.startswith("/"):
                            location = "{}{}".format(baseurl,location)
                        else:
                            location = "{}/{}".format(baseurl,location)
                    else:
                        errors.append("Service {0}({1}).{2}({3}): Location({4}) is not a valid http url".format(sectionindex,sectionid,serviceindex,serviceid,location))
                        continue

                if queryparameters:
                    if "?" in location:
                        location = "{}&{}".format(location,urllib.parse.urlencode(queryparameters))
                    else:
                        location = "{}?{}".format(location,urllib.parse.urlencode(queryparameters))

                service["location"] = location
    
                if not service.get("healthchecks"):
                    errors.append("Service {0}({1}).{2}({3}): Missing healthcheck configuration".format(sectionindex,sectionid,serviceindex,serviceid))
                    continue
                
                if not isinstance(service["healthchecks"],dict):
                    errors.append("Service {0}({1}).{2}({3}): Healthcheck is not a dictionary object".format(sectionindex,sectionid,serviceindex,serviceid))
                    continue
    
                try:
                    interval = service.get("interval")
                    if interval:
                        if not isinstance(interval,int):
                            interval = int(str(interval).strip())
                            service["interval"] = interval
                    elif not baseinterval:
                        errors.append("Service {0}({1}).{2}({3}): Missing property(interval)".format(sectionindex,sectionid,serviceindex,serviceid))
                        continue
                    else:
                        service["interval"] = baseinterval
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The property 'interval'({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("interval")))
                    continue

                try:
                    retry = service.get("retry")
                    if retry is None:
                        retry = baseretry
                    elif not isinstance(retry,int):
                        retry = int(str(retry).strip())
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The property 'retry'({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("retry")))
                    retry = baseretry
                    continue
                service["retry"] = retry

                try:
                    retryinterval = service.get("retry_interval")
                    if not retryinterval:
                        retryinterval = baseretryinterval
                    elif not isinstance(retryinterval,int):
                        retryinterval = int(str(retryinterval).strip()) / 1000
                    else:
                        retryinterval = retryinterval / 1000
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The property 'retry_interval'({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("retry_interval")))
                    retryinterval = baseretryinterval
                    continue
                service["retry_interval"] = retryinterval

                try:
                    timeout = service.get("timeout")
                    if timeout is None:
                        timeout = basetimeout
                    elif not isinstance(timeout,int):
                        timeout = int(str(timeout).strip())
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The property 'timeout'({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("timeout")))
                    timeout = basetimeout
                    continue
                service["timeout"] = timeout
                service["total_retry_processtime"] = (settings.CONNECT_TIMEOUT + timeout) / 1000.0 + retryinterval
                service["request_timeout"] = httpx.Timeout(connect = settings.CONNECT_TIMEOUT / 1000.0, read=timeout / 1000.0,write=timeout / 1000.0,pool=timeout/1000.0)

                try:
                    historyexpire = service.get("historyexpire")
                    if historyexpire is not None:
                        if not isinstance(historyexpire,int):
                            historyexpire = int(str(historyexpire).strip())
 
                        if historyexpire < 0:
                            historyexpire = 0
                        service["historyexpire"] = historyexpire
                    else:
                        service["historyexpire"] = basehistoryexpire
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The historyexpire({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("historyexpire")))
                    continue

                try:
                    errorhistoryexpire = service.get("errorhistoryexpire")
                    if errorhistoryexpire is not None:
                        if not isinstance(errorhistoryexpire,int):
                            errorhistoryexpire = int(str(errorhistoryexpire).strip())
 
                        if errorhistoryexpire < 0:
                            errorhistoryexpire = 0
                        service["errorhistoryexpire"] = errorhistoryexpire
                    elif baseerrorhistoryexpire:
                        service["errorhistoryexpire"] = baseerrorhistoryexpire
                    else:
                        service["errorhistoryexpire"] = historyexpire
                except Exception as ex:
                    errors.append("Service {0}({1}).{2}({3}): The errorhistoryexpire({4}) is not an integer".format(sectionindex,sectionid,serviceindex,serviceid,service.get("errorhistoryexpire")))
                    continue

                healthdetailpersistent = service.get("healthdetailpersistent")
                if healthdetailpersistent is not None:
                    if not healthdetailpersistent:
                        healthdetailpersistent = []
                    elif isinstance(healthdetailpersistent,str) and healthdetailpersistent == "__all__":
                        healthdetailpersistent = ["green","yellow","red","error"]
                    else:
                        if isinstance(healthdetailpersistent,str):
                            healthdetailpersistent = [s for s in healthdetailpersistent.split(",")]
                        data = set()
                        for s in healthdetailpersistent:
                            s = s.strip().lower()
                            if not s:
                                continue
                            if s not in ["green","yellow","red","error"]:
                                errors.append("Service {0}({1}).{2}({3}): The health status({4}) in property('healthdetailpersistent') doesn't' support".format(sectionindex,sectionid,serviceindex,serviceid,s))
                            else:
                                data.add(s)
                        healthdetailpersistent = data
                else:
                    healthdetailpersistent = basehealthdetailpersistent

                service["healthdetailpersistent"] = healthdetailpersistent

                service["section"] = config
                service = ServiceHealthCheck(self,service)

                #merge the sector's prtg config into service's prtg config
                reserved_prtg_channelids = set(RESERVED_PRTG_CHANNELS.keys())
                reserved_prtg_channelnames = dict()
                for k,func in RESERVED_PRTG_CHANNELNAME.items():
                    reserved_prtg_channelnames[k] = func(service)

                if service.get("prtg"):
                    if not isinstance(service["prtg"],list):
                        service["prtg"] = [service["prtg"]]

                    failed = False

                    for prtgconfig in service["prtg"]:
                        if "id" in prtgconfig:
                            channelid = prtgconfig.get("id")
                            if not self.ID_RE.search(channelid):
                                errors.append("Service {0}({1}).{2}: The channel id({3}) can only contain letters, numbers,'-' and '_'".format(sectionindex,sectionid,serviceid,channelid))
                                failed = True
                                break
                        elif len(service["prtg"]) == 1:
                            #only have one channle, use serviceid as default channle id
                            prtgconfig["id"] == serviceid
                        else:
                            errors.append("Service {0}({1}).{2}: Missing property 'id' in prtg config".format(sectionindex,sectionid,serviceid))
                            failed = True
                            break

                        for k in prtgconfig.keys():
                            v = prtgconfig[k]
                            if not v:
                                continue
                            if not isinstance(v,str):
                                continue
                            v = v.strip()
                            if v.startswith("lambda"):
                                try:
                                    prtgconfig[k] = eval(v)
                                except:
                                    errors.append("Service {0}({1}).{2}({3}): the lambda expression({5}) of the prtg data key({4}) is invalid.".format(sectionindex,sectionid,serviceindex,serviceid,k,v))
                                    failed = True
                                    break
                        if failed:
                            break


                        #find the baseconfig
                        baseconfig = baseprtgconfig.get(prtgconfig["id"]) if baseprtgconfig else None

                        if not baseconfig:
                            continue
                        #add the base config to service prtg config if it doesn't exist
                        for k,v in baseconfig.items():
                            if k not in prtgconfig:
                                prtgconfig[k] = v
                    if failed:
                        continue
                else:
                    service["prtg"] = None

                prtgconfigmap = OrderedDict()
                if service["prtg"]:
                    #initialize the final prtg config
                    #convert the prtg config from list to dict
                    for prtgconfig in service["prtg"]:
                        prtgconfig["channel"] = prtgconfig.get("channel") or service["name"]
                        prtgconfig["unit"] = prtgconfig.get("unit") or "Custom"

                        if  prtgconfig["unit"].lower() == "custom":
                            prtgconfig["customunit"] = prtgconfig.get("customunit") or "status"
                        elif "customunit" in prtgconfig:
                            del  prtgconfig["customunit"]

                        prtgconfig["value"] = prtgconfig.get("value") or 0
                        channelid = prtgconfig.pop("id")

                        getdata_map = {}
                        for status in ("green","yellow","red","error"):
                            key = "data4{}".format(status)
                            if key in prtgconfig:
                                getdata_map[status] = prtgconfig.pop(key)

                        computed_columns = {}
                        for k,v in prtgconfig.items():
                            if callable(v):
                                #turn the func with two arguments to a func with one argument if required
                                computed_columns[k] = checks.lambda_func_fatctory(service,v)

                        for k in computed_columns.keys():
                            del prtgconfig[k]

                        prtgconfigmap[channelid] = (prtgconfig,getdata_map,computed_columns)

                        #remove it from reserved prtg channel set to avoid to add this reserved prtg channel again.
                        reserved_prtg_channelids.discard(channelid)
                        if prtgconfig["channel"] in reserved_prtg_channelnames:
                            #the default channel name used by reserved prtg channels is configured in service prtg data, ignore the related reserved prtd data
                            reserved_prtg_channelids.discard(reserved_prtg_channelnames[prtgconfig["channel"]])

                #Add the RESERVED PRTG DATA if still has some reserved prtg channels are not added.
                for channelid in reserved_prtg_channelids:
                    prtgconfig = dict(baseprtgconfig[channelid])
                    prtgconfig["channel"] = RESERVED_PRTG_CHANNELNAME[channelid](service)
                    prtgconfigmap[channelid] = (prtgconfig,{},{})

                service["prtg"] = prtgconfigmap


                if not service["healthchecks"]:
                    errors.append("Service {0}({1}).{2}({3}): Missing healthcheck configuration".format(sectionindex,sectionid,serviceindex,serviceid))
                    continue

                for key in list(service["healthchecks"].keys()):
                    try:
                        if key not in ("green","yellow","red","error"):
                            errors.append("Service {0}({1}).{2}({3}): The health status({4}) in healthchecks is not in ('green','yellow','red','error')".format(sectionindex,sectionid,serviceindex,serviceid,key))
                            del config["services"][serviceid]["healthchecks"][key]
                            continue

                        if service["prtg"]:
                            prtgdata_map = {}
                            if isinstance(service["healthchecks"][key],(list,tuple)):
                                prtgdata_config = {}
                            else:
                                prtgdata_config = service["healthchecks"][key].get('prtg',{})
                                if not isinstance(prtgdata_config,dict):
                                    if len(service["prtg"]) > 1 :
                                        errors.append("Service {0}({1}).{2}({3}).{4}: The service declares multiple prtg channels ,the prtg config({5}) should be dict type".format(sectionindex,sectionid,serviceindex,serviceid,key,prtgdata_config))
                                        prtgdata_config = {}
                                    else:
                                        channelid = next(k for k in service["prtg"].keys())
                                        prtgdata_config = {channelid:prtgdata_config}


                            for channelid,prtgconfig in service["prtg"].items():
                                if channelid in prtgdata_config:
                                    prtgdata_map[channelid] = checks.get_prtg_factory(service,prtgdata_config[channelid]) 
                                elif key in prtgconfig[1]:
                                    prtgdata_map[channelid] = checks.get_prtg_factory(service,prtgconfig[1][key]) 

                            if isinstance(service["healthchecks"][key],(list,tuple)):
                                service["healthchecks"][key] = [
                                    checks.init_conds(service,service["healthchecks"][key]),
                                    checks.get_message_factory(service,None),
                                    prtgdata_map,
                                    None
                                ]
                            else:
                                service["healthchecks"][key] = [
                                    checks.init_conds(service,service["healthchecks"][key].get("condition")),
                                    checks.get_message_factory(service,service["healthchecks"][key].get('message')),
                                    prtgdata_map,
                                    checks.init_transforms(service["healthchecks"][key].get("transforms")) if service["healthchecks"][key].get("transforms") else None
                                ]
                        else:
                            if isinstance(service["healthchecks"][key],(list,tuple)):
                                service["healthchecks"][key] = [
                                    checks.init_conds(service,service["healthchecks"][key]),
                                    checks.get_message_factory(service,None),
                                    None,
                                    None
                                ]
                            else:
                                service["healthchecks"][key] = [
                                    checks.init_conds(service,service["healthchecks"][key].get("condition")),
                                    checks.get_message_factory(service,service["healthchecks"][key].get('message')),
                                    None,
                                    checks.init_transforms(service["healthchecks"][key].get("transforms")) if service["healthchecks"][key].get("transforms") else None
                                ]
                    except Exception as ex:
                        traceback.print_exc()
                        errors.append("Service {0}({1}).{2}({3}): The config({4}) in healthchecks is in valid.{5}: {6}".format(sectionindex,sectionid,serviceindex,serviceid,key,ex.__class__.__name__,str(ex)))
                        continue
    
                if not service["healthchecks"]:
                    continue
    

                services[serviceid] = service

            if not services:
                continue

            config["services"] = services
            sections[sectionid] = config
            
        return (sections,errors)



    def check(self,runner,taskcls,*args):
        if not self._checkingstatus_loaded:
            self.load_checkingstatus()

        for section in self.sections.values():
            if not section.enabled:
                continue
            for service in section["services"].values():
                if not service.enabled:
                    continue
                runner.add_task(taskcls(service,*args))

    def _schedule_continuous_check(self,taskcls,*args):
        shutdown.unregister_scheduled_task(self._continuous_check_task)
        self._continuous_check_task = asyncio.create_task(self._continuous_check(taskcls,*args))

    def stop_continuous_check(self):
        if not self._continuous_check_task:
            logger.debug("{}: The continuous health check has already been stopped.".format(self))
            return

        shutdown.unregister_scheduled_task(self._continuous_check_task)
        self._continuous_check_task.cancel()
        self._continuous_check_task = None
        logger.debug("{}: Stop continuous health check".format(self))

    async def _continuous_check(self,taskcls,*args):
        if shutdown.shutdowning:
            logger.info("The continuous health checking is end.")
            self._continuous_check_task = None
            return
        
        if not self._continuous_check_task:
            #already stopped
            return

        now = utils.now()
        today = datetime(year = now.year,month=now.month,day=now.day,tzinfo=settings.TZ)
        tomorrow = today + timedelta(days=1)
        seconds_in_day = int((now - today).total_seconds())

        self._next_runtime = None
        for section in self.sections.values():
            if not section.enabled:
                continue
            for service in section["services"].values():
                if not service.enabled:
                    continue
                if now >= service.healthstatus_nextchecktime:
                    #check this service now
                    if service.healthstatus_running:
                        #the healthcheck of this service is running now
                        #can't launch another healthcheck. delay 5 seconds and try again
                        next_checktime = now + timedelta(seconds=5)
                    else:
                        #the healthcheck of this service is not running
                        logger.debug("{} : Run a task to check the service({}.{}.lastchecktime = {}, next checktime={})  to task runner.".format(self,service.sectionid,service.serviceid,service.healthstatus_nextcheck,service.get_nextchecktime(service["offset"],service.healthstatus_nextchecktime,now,today,tomorrow,seconds_in_day)))
                        service.healthstatus_running = True
                        task = taskcls(service,*args)
                        asyncio.create_task(task.run())
                        next_checktime = service.get_nextchecktime(service["offset"],service.healthstatus_nextchecktime,now,today,tomorrow,seconds_in_day)
                        #set the nextchecktime
                        service.healthstatus_nextchecktime = next_checktime
                else:
                    next_checktime = service.healthstatus_nextchecktime
                if not self._next_runtime or self._next_runtime > next_checktime:
                    self._next_runtime = next_checktime

        if not self._continuous_check_task:
            #already stopped
            return

        if not self._next_runtime:
            self._next_runtime = now + timedelta(seconds=30)

        seconds = (self._next_runtime - utils.now()).total_seconds()
        if seconds > 0:
            logger.debug("Waiting {} seconds to begin the next batch of service health check.".format(seconds))
            self._continuous_check_task = asyncio.get_running_loop().call_later(seconds,self._schedule_continuous_check,taskcls,*args)
            shutdown.register_scheduled_task(self._continuous_check_task)
        else:
            self._continuous_check_task = asyncio.create_task(self._continuous_check(taskcls,*args))

    @property
    def is_continuous_check_started(self):
        return self._continuous_check_task != None

    _continuous_check_task = None
    async def continuous_check(self,*args,taskcls=BaseServiceHealthCheckTask):
        if not self._checkingstatus_loaded:
            self.load_checkingstatus()
        if self._continuous_check_task:
            logger.info("{}: The continuous health checking is already started.".format(self))
            return 
        logger.info("{}: Start to run the continuous health checking".format(self))
        self._continuous_check_task = asyncio.create_task(self._continuous_check(taskcls,*args))


    @classmethod
    def populate_healthstatus(cls,serviceconfig,status,message,attempts,processtime):
        """
        Get the healthstatus for exception
        """
        if serviceconfig["prtg"]:
            prtgdata = {}
            for channelid in serviceconfig["prtg"].keys():
                if channelid == "__status__":
                    prtgdata[channelid] = 3 if status == "green" else (2 if status == "yellow" else (1 if status == "red" else 0))
                elif channelid == "__processtime__":
                    prtgdata[channelid] = int(processtime) #milliseconds
                elif channelid == "__attempts__":
                    prtgdata[channelid] = attempts
        else:
            prtgdata = PRTGDATA_NOT_ENABLED

        return [status,message,prtgdata]

    @classmethod
    def check_response(cls,serviceconfig,res,attempts,processtime):
        """
        Return [traffic light, msgs,prtg data]
        """
        healthstatus = None
        messages = [] if settings.HEALTHCHECK_CONDITION_VERBOSE else None
        for key in ("green","yellow","red","error"):
            if key not in serviceconfig["healthchecks"]:
                continue
            checkconditions,get_checkmessage,get_prtgdata,transforms = serviceconfig["healthchecks"][key]
            try:
                if settings.HEALTHCHECK_CONDITION_VERBOSE:
                    messages.clear()
                #transform the response
                #transformed response should be compatible with the original response
                if transforms:
                    for transform in transforms:
                        res = transform(res)

                checkresult =  checks.check(res,checkconditions,messages=messages)
                if checkresult:
                    checkmsg = get_checkmessage(res)
                    if serviceconfig["prtg"]:
                        prtgdata = {}
                        for channelid in serviceconfig["prtg"].keys():
                            if get_prtgdata.get(channelid):
                                prtgdata[channelid] = get_prtgdata[channelid](res)
                            elif channelid == "__status__":
                                prtgdata[channelid] = 3 if key == "green" else (2 if key == "yellow" else (1 if key == "red" else 0))
                            elif channelid == "__processtime__":
                                prtgdata[channelid] = int(processtime) #milliseconds
                            elif channelid == "__attempts__":
                                prtgdata[channelid] = attempts
                    else:
                        prtgdata = PRTGDATA_NOT_ENABLED

                    if not isinstance(checkmsg,str):
                        checkmsg = json.dumps(checkmsg,indent=4,cls=serializers.JSONFormater)
                    if settings.DEBUG and messages:
                        msg = "{}\nThe following conditions are checked.\n    {}".format(checkmsg,"\n    ".join(messages))
                    else:
                        msg = checkmsg
                    healthstatus = [key,msg,prtgdata]
                    break
                else:
                    """
                    if settings.DEBUG:
                        msg = "The healthstatus({}) are not satified.\n    {}\n    The following conditions are checked.\n        {}".format(key,get_checkmessage(res),"\n        ".join(messages))
                    else:
                        msg = "The healthstatus({}) are not satified.\n    {}".format(key,get_checkmessage(res))
                    logger.debug(msg)
                    """
                    pass

            except Exception as ex:
                traceback.print_exc()
                healthstatus = cls.populate_healthstatus(serviceconfig,"error","Failed to check health status '{}'.{}:{}".format(key,ex.__class__.__name__,str(ex)),attempts,processtime)
                break
    
        if not healthstatus:
            if res:
                contenttype = res.headers.get("Content-Type","").lower()
                if any( (key in contenttype) for key in ("text","json","xml")):
                    message = res.text
                else:
                    message = "non-text response"
                healthstatus = cls.populate_healthstatus(serviceconfig,"error","Status Code:{}, Message:{}".format(res.status_code,message),attempts,processtime)
            else:
                healthstatus = cls.populate_healthstatus(serviceconfig,"error","All healthstatus configured in {} are not satisfied.",attempts,processtime)


        if attempts > 1:
            healthstatus[1] = "{1}. Attempts:{0}".format(attempts,healthstatus[1])
    
        return healthstatus

class EditingHealthCheck(HealthCheck):

    def __init__(self,healthcheck,configfile):
        super().__init__(configfile=configfile)
        self.healthcheck = healthcheck
        self._lock = FileLock("{}.lock".format(self.configfile))
        self.load_checkingstatus()
        self._continuouscheck_starttime = None
        

    def reset(self):
        """
        Return True if changed; otherwise return False
        """
        with self._lock:
            if os.path.exists(self.healthcheck.configfile):
                shutil.copyfile(self.healthcheck.configfile,self.configfile)
            else:
                with open(self.configfile,'w') as f:
                    f.write("[]")

            with open(self.configfile,'rb') as f:
                configs = f.read()
            config_hashcode = hashlib.sha256(configs).hexdigest()
            return config_hashcode != self.config_hashcode

    async def continuous_check(self,*args,taskcls=BaseServiceHealthCheckTask):
        await super().continuous_check(*args,taskcls=taskcls)
        self._continuouscheck_starttime = utils.now()

    def stop_continuous_check(self):
        super().stop_continuous_check()
        self._continuouscheck_starttime = None

    async def _continuous_check(self,taskcls,*args):
        if self._continuouscheck_starttime and (utils.now() - self._continuouscheck_starttime).total_seconds() > settings.EDITINGHEALTHCHECK_CONTINUOUSCHECK_MAXTIME:
            #exceed the maximum time
            self.stop_continuous_check()
            return

        await super()._continuous_check(taskcls,*args)

    def save(self,configtxt):
        """
        Return True if changed; otherwise return False
        """
        #parse
        with self._lock:
            try:
                configs = json.loads(configtxt)
            except Exception as ex:
                raise Exception("Health check configuration is not a correct json object")
            #check the configuration
            data,errors = self.init_configs(configs)
            if errors:
                raise Exception("""Found {1} issues in healthcheck configuration file({0})
        {2}
    """.format(self.configfile,len(errors),"\n    ".join(errors)))
    
            with open(self.configfile,'w') as f:
                f.write(configtxt)
    
            with open(self.configfile,'rb') as f:
                configs = f.read()
            config_hashcode = hashlib.sha256(configs).hexdigest()
            return config_hashcode != self.config_hashcode

    def publish(self,user,comments):
        #validate the healthcheck configuration file
        with self._lock:
            try:
                with open(self.configfile,'rb') as f:
                    configtxt = f.read()
    
                config_hashcode = hashlib.sha256(configtxt).hexdigest()
                if config_hashcode == self.healthcheck.config_hashcode:
                    #not changed
                    changed = False
                    utils.remove_file(self.configfile)
                    return False
                    
                configs = json.loads(configtxt.decode())
            except Exception as ex:
                raise Excepton("Health check configuration is not a correct json object")
    
            data,errors = self.init_configs(configs)
            if errors:
                raise Exception("""Found {1} issues in healthcheck configuration file({0})
        {2}
    """.format(self.configfile,len(errors),"\n    ".join(errors)))
    
            #save the publish config file
            now = utils.now()
            configdir,configfilename = os.path.split(self.healthcheck.configfile)
            configfilebase,configfileext = os.path.splitext(configfilename)
            publishedfile = os.path.join(self.healthcheck.publishhistorydir,"{1}.{0}{2}".format(now.strftime("%Y%m%dT%H%M%S"),configfilebase,configfileext))
            shutil.copyfile(self.configfile,publishedfile)
    
            #update the publish history
            count = 0
            tmpfile = "{}.tmp".format(self.healthcheck.publishhistoryfile)
            with open(tmpfile,'w') as fw:
                count += 1
                fw.write(json.dumps([now.strftime("%Y-%m-%d %H:%M:%S"),os.path.basename(publishedfile),user,comments]))
                with open(self.healthcheck.publishhistoryfile,'r') as fr:
                    while True:
                        line = fr.readline()
                        if not line:
                            break
                        else:
                            count += 1
                            if count == 2:
                                fw.write(os.linesep)
                            if count <= settings.HEALTHCHECK_PUBLISH_HISTORIES:
                                fw.write(line)
                            else:
                                #remove the publish history
                                publisheddata = json.loads(line.strip())
                                if publisheddata[1]:
                                    utils.remove_file(os.path.join(self.healthcheck.publishhistorydir,publisheddata[1]))
            os.rename(tmpfile,self.healthcheck.publishhistoryfile)
    
            #update the current healthceck config file
            os.rename(self.configfile,self.healthcheck.configfile)
            return True

class SystemViewMeta(list):
    @property
    def id(self):
        return self[0]

    @id.setter
    def id(self,v):
        self[0] = v


    @property
    def title(self):
        return self[1]

    @title.setter
    def title(self,v):
        self[1] = v

    @property
    def description(self):
        return self[2]

    @description.setter
    def description(self,v):
        self[2] = v

class PRTGSensorMeta(SystemViewMeta):
    pass

class UserViewMeta(object):
    def __init__(self,user,healthcheck):
        self._user = user
        self._healthcheck = healthcheck

    def __str__(self):
        return self._user

    @property
    def title(self):
        return "Systems Health Check -- {}".format(self._user)

    @property
    def description(self):
        return "The view contains only user interested systems"

class SelectablePRTGChannel(object):
    def __init__(self,channelid,channelname,selected):
        self.channelid = channelid
        self.channelname = channelname
        self.selected = selected


class ServiceHealthCheckPRTGSensor(object):
    def __init__(self,servicehealthcheck,channelsettings):
        self._servicehealthcheck = servicehealthcheck
        self._channelsettings = channelsettings

    def __getattr__(self,name):
        return getattr(self._servicehealthcheck,name)

    def __getitem__(self,name):
        return self._servicehealthcheck.get(name)

    @property
    def prtgchannels(self):
        if not self._channelsettings:
            return []
        return filter(lambda item: item[0] in self._channelsettings, self._servicehealthcheck.prtgchannels)

    @property
    def selectableprtgchannels(self):
        return map(lambda item: SelectablePRTGChannel(item[0],item[1][0]["channel"],self._channelsettings is not None and item[0] in self._channelsettings),self._servicehealthcheck.prtgchannels)

class SelectableServiceHealthCheck(object):
    def __init__(self,servicehealthcheck,selected):
        self._servicehealthcheck = servicehealthcheck
        self.selected = selected

    def __getattr__(self,name):
        return getattr(self._servicehealthcheck,name)

    def __getitem__(self,name):
        return self._servicehealthcheck.get(name)


class SectionHealthCheckView(object):
    def __init__(self,sectionhealthcheck,viewsettings):
        self._sectionhealthcheck = sectionhealthcheck
        self._viewsettings = viewsettings

    def __getattr__(self,name):
        return getattr(self._sectionhealthcheck,name)

    def __getitem__(self,name):
        return self._sectionhealthcheck.get(name)

    @property
    def healthcheckservices(self):
        if self._viewsettings is not None and not self._viewsettings:
            return []
        return filter(lambda service: self._viewsettings is None or service.serviceid in self._viewsettings, self._sectionhealthcheck.healthcheckservices)

    @property
    def selectablehealthcheckservices(self):
        return map(lambda service: SelectableServiceHealthCheck(service,self._viewsettings is not None and service.serviceid in self._viewsettings),self._sectionhealthcheck.healthcheckservices)

class SectionHealthCheckPRTGSensor(object):
    def __init__(self,sectionhealthcheck,sensorsettings):
        self._sectionhealthcheck = sectionhealthcheck
        self._sensorsettings = sensorsettings

    def __getattr__(self,name):
        return getattr(self._sectionhealthcheck,name)

    def __getitem__(self,name):
        return self._sectionhealthcheck.get(name)

    @property
    def healthcheckservices(self):
        if not self._sensorsettings:
            return []
        return map(lambda service: ServiceHealthCheckPRTGSensor(service,self._sensorsettings.get(service.serviceid,None) ), filter(lambda service: service.prtgenabled and service.serviceid in self._sensorsettings, self._sectionhealthcheck.healthcheckservices))

    @property
    def selectablehealthcheckservices(self):
        return map(lambda service: ServiceHealthCheckPRTGSensor(service,self._sensorsettings.get(service.serviceid,None) if self._sensorsettings else None ), filter(lambda service:service.prtgenabled,self._sectionhealthcheck.healthcheckservices))


class HealthCheckView(PRTGMixin,JsonStatusMixin):
    def __init__(self,healthcheck,viewmeta,viewsettings):
        self._healthcheck = healthcheck
        self._viewmeta = viewmeta
        self._viewsettings = viewsettings

    @property
    def title(self):
        return self._viewmeta.title if self._viewmeta else ""

    @property
    def healthchecksections(self):
        if not self._viewsettings:
            return self._healthcheck.healthchecksections
        return map(lambda section: SectionHealthCheckView(section,self._viewsettings.get(section.sectionid,set()) if self._viewsettings else None ), filter(lambda section: section.sectionid in self._viewsettings, self._healthcheck.healthchecksections))

    @property
    def selectablehealthchecksections(self):
        return map(lambda section: SectionHealthCheckView(section,self._viewsettings.get(section.sectionid,set()) if self._viewsettings else None ), self._healthcheck.healthchecksections)

class HealthCheckPRTGSensor(PRTGMixin,JsonStatusMixin):
    def __init__(self,healthcheck,sensormeta,sensorsettings):
        self._healthcheck = healthcheck
        self._sensormeta = sensormeta
        self._sensorsettings = sensorsettings

    @property
    def title(self):
        return self._sensormeta.title if self._sensormeta else ""

    @property
    def healthchecksections(self):
        if not self._sensorsettings:
            return []
        return map(lambda section: SectionHealthCheckPRTGSensor(section,self._sensorsettings.get(section.sectionid,{}) ), filter(lambda section: section.prtgenabled and self._sensorsettings and section.sectionid in self._sensorsettings, self._healthcheck.healthchecksections))

    @property
    def selectablehealthchecksections(self):
        return map(lambda section: SectionHealthCheckPRTGSensor(section,self._sensorsettings.get(section.sectionid,{}) if self._sensorsettings else None ), filter(lambda section: section.prtgenabled,self._healthcheck.healthchecksections))

class ReleasedHealthCheck(HealthCheck):

    def __init__(self):
        super().__init__(configfile=settings.HEALTHCHECK_CONFIGFILE)
        self._views = {}
        self._prtgsensorsconfig = {}
        

    _editconfigdir = None
    @property
    def editconfigdir(self):
        if not self._editconfigdir:
            configdir,configfilename = os.path.split(self.configfile)
            self._editconfigdir = configdir
        return self._editconfigdir

    _editconfigfile = None
    @property
    def editconfigfile(self):
        if not self._editconfigfile:
            configfilename = os.path.basename(self.configfile)
            configfilebase,configfileext = os.path.splitext(configfilename)
            f = os.path.join(self.editconfigdir,"{}.edit{}".format(configfilebase,configfileext))
            self._editconfigfile = f

        if not os.path.exists(self._editconfigfile):
            if os.path.exists(self.configfile):
                shutil.copyfile(self.configfile,self._editconfigfile)
            else:
                with open(self._editconfigfile,'w') as fw:
                    fw.write("[]")
        elif not os.path.isfile(self._editconfigfile):
            raise Exception("The editing config file({}) is not a file".format(self._editconfigfile))

        return self._editconfigfile

    _publishhistorydir = None
    @property
    def publishhistorydir(self):
        if not self._publishhistorydir:
            configdir,configfilename = os.path.split(self.configfile)
            configfilebase,configfileext = os.path.splitext(configfilename)
            folder = os.path.join(configdir,"{}.publishhistories".format(configfilebase))
            utils.makedir(folder)
            self._publishhistorydir = folder
        return self._publishhistorydir

    _publishhistoryfile = None
    @property
    def publishhistoryfile(self):
        if not self._publishhistoryfile:
            tmpfile = os.path.join(self.publishhistorydir,"publishhistories.json")
            if not os.path.exists(tmpfile):
                configfilename = os.path.basename(self.configfile)
                configfilebase,configfileext = os.path.splitext(configfilename)
                initialfile = os.path.join(self.publishhistorydir,"{}.initial{}".format(configfilebase,configfileext))
                if self.sections:
                    shutil.copyfile(self.configfile,initialfile)
                    with open(tmpfile,'w') as f:
                        f.write(json.dumps(["Initial",os.path.basename(initialfile),"","Initial"]))
                else:
                    with open(tmpfile,'w') as f:
                        pass
            elif not os.path.isfile(tmpfile):
                raise Exception("The publish history file({}) is not a file".format(tmpfile))
            self._publishhistoryfile = tmpfile

        return self._publishhistoryfile

    _editing_healthcheck = None
    @property
    def editing_healthcheck(self):
        if not self._editing_healthcheck:
            self._editing_healthcheck = EditingHealthCheck(self,self.editconfigfile)

        return self._editing_healthcheck


    @property
    def publishhistories(self):
        histories = []
        with open(self.publishhistoryfile,'r') as fr:
            while True:
                line = fr.readline()
                if not line:
                    break
                else:
                    try:
                        histories.append(json.loads(line.strip()))
                    except Exception as ex:
                        logger.error("{}: Failed to parse the published healthcheck configuration data ({})".format(self,line))

        return histories

    def rollback(self,configfile):
        configfile = os.path.join(self.publishhistorydir,configfile)
        if not os.path.exists(configfile):
            raise Exception("{}: The config file({}) doesn't exist".format(self,configfile))

        with open(configfile,'rb') as f:
            configtxt = f.read()

        config_hashcode = hashlib.sha256(configtxt).hexdigest()
        if config_hashcode == self.config_hashcode:
            logger.debug("{}: config file is not changed".format(self))
            return False

        shutil.copyfile(configfile,self.configfile)
        return True

    _systemviewsdir = None
    @property 
    def systemviewsdir(self):
        if not self._systemviewsdir:
            configdir,filename = os.path.split(self.configfile)
            basename = os.path.splitext(filename)[0]
            d = os.path.join(configdir,"{}.views".format(basename),"system")
            if not os.path.exists(d):
                utils.makedir(d)
            elif not os.path.isdir(d):
                raise Exception("The path({}) is not a folder".format(d))
            self._systemviewsdir = d

        return self._systemviewsdir

    _prtgsensorsdir = None
    @property 
    def prtgsensorsdir(self):
        if not self._prtgsensorsdir:
            configdir,filename = os.path.split(self.configfile)
            basename = os.path.splitext(filename)[0]
            d = os.path.join(configdir,"{}.prtgsensors".format(basename))
            if not os.path.exists(d):
                utils.makedir(d)
            elif not os.path.isdir(d):
                raise Exception("The path({}) is not a folder".format(d))
            self._prtgsensorsdir = d

        return self._prtgsensorsdir

    _userviewsdir = None
    @property 
    def userviewsdir(self):
        if not self._userviewsdir:
            configdir,filename = os.path.split(self.configfile)
            basename = os.path.splitext(filename)[0]
            d = os.path.join(configdir,"{}.views".format(basename),"user")
            if not os.path.exists(d):
                utils.makedir(d)
            elif not os.path.isdir(d):
                raise Exception("The path({}) is not a folder".format(d))
            self._userviewsdir = d

        return self._userviewsdir

    def get_viewdir(self,key):
        if "@" in key:
            #key is a user
            return self.userviewsdir
        else:
            #key is a system
            return self.systemviewsdir

    _viewsfile = None
    @property
    def systemviewsfile(self):
        if not self._viewsfile:
            self._viewsfile = os.path.join(self.systemviewsdir,"systemviews.json")
        return self._viewsfile

    _prtgsensorsfile = None
    @property
    def prtgsensorsfile(self):
        if not self._prtgsensorsfile:
            self._prtgsensorsfile = os.path.join(self.prtgsensorsdir,"prtgsensors.json")
        return self._prtgsensorsfile

    _systemviews = [None,None,[]]
    @property 
    def systemviews(self):
        if not os.path.exists(self.systemviewsfile):
            self._systemviews[0] = None
            self._systemviews[1] = None
            self._systemviews[2].clear()
            return self._systemviews[2]

        file_size = os.path.getsize(self.systemviewsfile)
        if not file_size:
            utils.remove_file(self.systemviewsfile)
            self._systemviews[0] = None
            self._systemviews[1] = None
            self._systemviews[2].clear()
            return self._systemviews[2]

        file_mtime = os.path.getmtime(self.systemviewsfile)
        if self._systemviews[0] != file_mtime  or self._systemviews[1] != file_size:
            systemviews = []
            with open(self.systemviewsfile,'r') as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        systemviews.append(SystemViewMeta(json.loads(line)))
                    except Exception as ex :
                        logger.error("{}: Failed to parse the system view({})".format(self,line))

            if not systemviews:
                #no system views
                utils.remove_file(self.systemviewsfile)
                self._systemviews = [None,None,None]
                return []
            self._systemviews[0] = file_mtime
            self._systemviews[1] = file_size
            self._systemviews[2] = systemviews

        return self._systemviews[2]

    _prtgsensors = [None,None,[]]
    @property 
    def prtgsensors(self):
        if not os.path.exists(self.prtgsensorsfile):
            self._prtgsensors[0] = None
            self._prtgsensors[1] = None
            self._prtgsensors[2].clear()
            return self._prtgsensors[2]

        file_size = os.path.getsize(self.prtgsensorsfile)
        if not file_size:
            utils.remove_file(self.prtgsensorsfile)
            self._prtgsensors[0] = None
            self._prtgsensors[1] = None
            self._prtgsensors[2].clear()
            return self._prtgsensors[2]

        file_mtime = os.path.getmtime(self.prtgsensorsfile)
        if self._prtgsensors[0] != file_mtime  or self._prtgsensors[1] != file_size:
            prtgsensors = []
            with open(self.prtgsensorsfile,'r') as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        prtgsensors.append(PRTGSensorMeta(json.loads(line)))
                    except Exception as ex :
                        logger.error("{}: Failed to parse the system view({})".format(self,line))

            if not prtgsensors:
                #no system views
                utils.remove_file(self.prtgsensorsfile)
                self._prtgsensors = [None,None,None]
                return []
            self._prtgsensors[0] = file_mtime
            self._prtgsensors[1] = file_size
            self._prtgsensors[2] = prtgsensors

        return self._prtgsensors[2]

    def get_viewsettings(self,key):
        """
        Return None if no customization, otherwise return {section:service set}
        """
        if not key:
            return None
        viewfile = os.path.join(self.get_viewdir(key),"{}.json".format(key))

        if not os.path.exists(viewfile):
            if key in self._views:
                del self._views[key]
            return None
        
        file_size = os.path.getsize(viewfile)
        file_mtime = os.path.getmtime(viewfile)
        if not file_size:
            #no customization
            utils.remove_file(viewfile)
            if key in self._views:
                del self._views[key]
            return None

        if key not in self._views or self._views[key][0] != file_mtime or self._views[key][1] != file_size:
            #file changed, reload the file
            with open(viewfile) as f:
                data = f.read()
            viewsettings = json.loads(data)
            #turn the service list to service set
            #remove the empty secion
            for sector in [k for k in viewsettings.keys()]:
                viewsettings[sector] = set(viewsettings[sector])
                if not viewsettings[sector]:
                    del viewsettings[sector]
            if viewsettings:
                self._views[key] = [file_mtime,file_size,viewsettings]
            else:
                #no customization
                utils.remove_file(viewfile)
                if key in self._views:
                    del self._views[key]
                return None


        return self._views[key][2]

    def get_prtgsensorsettings(self,key):
        """
        Return None if no customization, otherwise return {section:{service: sensor set}}
        """
        if not key:
            return None
        sensorfile = os.path.join(self.prtgsensorsdir,"{}.json".format(key))

        if not os.path.exists(sensorfile):
            if key in self._prtgsensorsconfig:
                del self._prtgsensorsconfig[key]
            return None
        
        file_size = os.path.getsize(sensorfile)
        file_mtime = os.path.getmtime(sensorfile)
        if not file_size:
            #no customization
            utils.remove_file(sensorfile)
            if key in self._prtgsensorsconfig:
                del self._prtgsensorsconfig[key]
            return None

        if key not in self._prtgsensorsconfig or self._prtgsensorsconfig[key][0] != file_mtime or self._prtgsensorsconfig[key][1] != file_size:
            #file changed, reload the file
            with open(sensorfile) as f:
                data = f.read()
            sensorsettings = json.loads(data)
            #turn the service list to service set
            #remove the empty secion
            for sector in [k1 for k1 in sensorsettings.keys()]:
                sectorsettings = sensorsettings[sector]
                for service in [k2 for k2 in sectorsettings.keys()]:
                    sectorsettings[service] = set(sectorsettings[service])
                    if not sectorsettings[service]:
                        del sectorsettings[service]
                if not sectorsettings:
                    del sensorsettings[sector]
            if sensorsettings:
                self._prtgsensorsconfig[key] = [file_mtime,file_size,sensorsettings]
            else:
                #no customization
                utils.remove_file(sensorfile)
                if key in self._prtgsensorsconfig:
                    del self._prtgsensorsconfig[key]
                return None

        return self._prtgsensorsconfig[key][2]

    def save_systemview(self,viewid,title,description):
        systemviews = self.systemviews
        systemview = next((v for v in systemviews if v.id == viewid),None)
        if systemview:
            if systemview.title != title or systemview.description != description:
                #changed
                systemview.title = title
                systemview.description = description
                firstline = True
                with open(self.systemviewsfile,'wb') as f:
                    for view in systemviews:
                        if not firstline:
                            f.write(b'\n')
                        else:
                            firstline = False
                        f.write(json.dumps(view).encode())

                
                self._systemviews[0] = os.path.getmtime(self.systemviewsfile)
                self._systemviews[1] = os.path.getsize(self.systemviewsfile)
                logger.debug("{}: Update the system views file({})".format(self,self.systemviewsfile))
        else:
            systemview = SystemViewMeta([viewid,title,description])
            with open(self.systemviewsfile,'ab') as f:
                if len(systemviews) > 0:
                    f.write(b'\n')
                f.write(json.dumps(systemview).encode())

            systemviews.append(systemview)
            self._systemviews[0] = os.path.getmtime(self.systemviewsfile)
            self._systemviews[1] = os.path.getsize(self.systemviewsfile)
            logger.debug("{}: Append the system views file({})".format(self,self.systemviewsfile))

    def save_prtgsensor(self,sensorid,title,description):
        prtgsensors = self.prtgsensors
        sensor = next((v for v in prtgsensors if v.id == sensorid),None)
        if sensor:
            if sensor.title != title or sensor.description != description:
                #changed
                sensor.title = title
                sensor.description = description
                firstline = True
                with open(self.prtgsensorsfile,'wb') as f:
                    for view in prtgsensors:
                        if not firstline:
                            f.write(b'\n')
                        else:
                            firstline = False
                        f.write(json.dumps(view).encode())

                
                self._prtgsensors[0] = os.path.getmtime(self.prtgsensorsfile)
                self._prtgsensors[1] = os.path.getsize(self.prtgsensorsfile)
                logger.debug("{}: Update the prtg sensor file({})".format(self,self.prtgsensorsfile))
        else:
            sensor = PRTGSensorMeta([sensorid,title,description])
            with open(self.prtgsensorsfile,'ab') as f:
                if len(prtgsensors) > 0:
                    f.write(b'\n')
                f.write(json.dumps(sensor).encode())

            prtgsensors.append(sensor)
            self._prtgsensors[0] = os.path.getmtime(self.prtgsensorsfile)
            self._prtgsensors[1] = os.path.getsize(self.prtgsensorsfile)
            logger.debug("{}: Append the prtg sensor file({})".format(self,self.prtgsensorsfile))

    def delete_systemview(self,viewid):
        systemviews = self.systemviews
        pos = next((i for i in range(len(systemviews)) if systemviews[i].id == viewid),-1)
        if pos == -1:
            return
        systemview = systemviews[pos]
        #remove system view settings in memory and delte setting file
        self.save_viewsettings(systemview.id)

        #delete from systemviews
        del systemviews[pos]
        firstline = True
        with open(self.systemviewsfile,'wb') as f:
            for view in systemviews:
                if not firstline:
                    f.write(b'\n')
                else:
                    firstline = False
                f.write(json.dumps(view).encode())
        self._systemviews[0] = os.path.getmtime(self.systemviewsfile)
        self._systemviews[1] = os.path.getsize(self.systemviewsfile)

    def delete_prtgsensor(self,sensorid):
        prtgsensors = self.prtgsensors
        pos = next((i for i in range(len(prtgsensors)) if prtgsensors[i].id == sensorid),-1)
        if pos == -1:
            return
        sensor = prtgsensors[pos]
        #remove system view settings in memory and delte setting file
        self.save_prtgsensorsettings(sensor.id)

        #delete from prtgsensors
        del prtgsensors[pos]
        firstline = True
        with open(self.prtgsensorsfile,'wb') as f:
            for sensor in prtgsensors:
                if not firstline:
                    f.write(b'\n')
                else:
                    firstline = False
                f.write(json.dumps(sensor).encode())
        self._prtgsensors[0] = os.path.getmtime(self.prtgsensorsfile)
        self._prtgsensors[1] = os.path.getsize(self.prtgsensorsfile)

    def save_viewsettings(self,key,viewsettings=None):
        #remove duplicate service, remove empty section
        viewfile = os.path.join(self.get_viewdir(key),"{}.json".format(key))
        if not viewsettings:
            #no customization
            utils.remove_file(viewfile)
            if key in self._views:
                del self._views[key]
            return

        #remove empty sectors
        for sector in [k for k in viewsettings.keys()]:
            if not isinstance(viewsettings[sector],set):
                viewsettings[sector] = set(viewsettings[sector])
            if not viewsettings[sector]:
                del viewsettings[sector]
        
        if not viewsettings:
            #no customization
            utils.remove_file(viewfile)
            if key in self._views:
                del self._views[key]
            return

        #check whether it is customized or not
        customized = False
        for section in self.healthchecksections:
            if section.sectionid not in viewsettings:
                customized = True
                break
            if len(section["services"]) != len(viewsettings[section.sectionid]):
                customized = True
                break

        #if not customized, remove the settings
        if not customized:
            #not customized
            util.remove_file(viewfile)
            if key in self._views:
                del self._views[key]
            return

        if key in self._views and self._views[key][2] == viewsettings:
            return

        #change the service set to service list
        for k in viewsettings.keys():
            viewsettings[k] = list(viewsettings[k])

        with open(viewfile,'w') as f:
            f.write(json.dumps(viewsettings,indent=4))

        #change the service list back to service set
        for k in viewsettings.keys():
            viewsettings[k] = set(viewsettings[k])

        if key in self._views:
            self._views[key][0] = os.path.getmtime(viewfile)
            self._views[key][1] = os.path.getsize(viewfile)
            self._views[key][2] = viewsettings
        else:
            self._views[key] = [os.path.getmtime(viewfile),os.path.getsize(viewfile),viewsettings]

        logger.debug("{}: Changed the settings for view({})".format(self,key))

    def save_prtgsensorsettings(self,key,sensorsettings=None):
        #remove duplicate service, remove empty section
        sensorfile = os.path.join(self.prtgsensorsdir,"{}.json".format(key))
        if not sensorsettings:
            #no customization
            utils.remove_file(sensorfile)
            if key in self._prtgsensorsconfig:
                del self._prtgsensorsconfig[key]
            return

        for sector in [k1 for k1 in sensorsettings.keys()]:
            sectorsettings = sensorsettings[sector]
            for service in [k2 for k2 in sectorsettings.keys()]:
                if not isinstance(sectorsettings[service],set):
                    sectorsettings[service] = set(sectorsettings[service])
                if not sectorsettings[service]:
                    del sectorsettings[service]
            if not sectorsettings:
                del sensorsettings[sector]
        
        if not sensorsettings:
            #no customization
            utils.remove_file(sensorfile)
            if key in self._prtgsensorsconfig:
                del self._prtgsensorsconfig[key]
            return

        if key in self._prtgsensorsconfig and self._prtgsensorsconfig[key][2] == sensorsettings:
            return

        #change the service set to service list
        for sectorsettings in sensorsettings.values():
            for k in sectorsettings.keys():
                sectorsettings[k] = list(sectorsettings[k])

        with open(sensorfile,'w') as f:
            f.write(json.dumps(sensorsettings,indent=4))

        #change the service list back to service set
        for sectorsettings in sensorsettings.values():
            for k in sectorsettings.keys():
                sectorsettings[k] = set(sectorsettings[k])

        if key in self._prtgsensorsconfig:
            self._prtgsensorsconfig[key][0] = os.path.getmtime(sensorfile)
            self._prtgsensorsconfig[key][1] = os.path.getsize(sensorfile)
            self._prtgsensorsconfig[key][2] = sensorsettings
        else:
            self._prtgsensorsconfig[key] = [os.path.getmtime(sensorfile),os.path.getsize(sensorfile),sensorsettings]

        logger.debug("{}: Changed the settings for prtg sensor({})".format(self,key))

    def get_viewmeta(self,viewid):
        if "@" in viewid:
            return UserViewMeta(viewid,self)
        else:
            return next((v for v in self.systemviews if v.id == viewid),None)

    def get_prtgsensormeta(self,sensorid):
        return next((v for v in self.prtgsensors if v.id == sensorid),None)

    def get_view(self,viewid):
        if not viewid:
            return self
        viewmeta = self.get_viewmeta(viewid)
        viewsettings = self.get_viewsettings(viewid)
        return HealthCheckView(self,viewmeta,viewsettings)

    def get_prtgsensor(self,sensorid):
        if not sensorid:
            raise Exception("Missing sensor id.")
        sensormeta = self.get_prtgsensormeta(sensorid)
        if not sensormeta:
            raise Exception("PRTG sensor({}) doesn't exist.".format(sensorid))

        sensorsettings = self.get_prtgsensorsettings(sensorid)
        return HealthCheckPRTGSensor(self,sensormeta,sensorsettings)

healthcheck = ReleasedHealthCheck()

async def main():
    try:
        await healthcheck.continuous_check()
        await shutdown.wait()
    except exceptions.SystemShutdown as ex:
        pass
    except asyncio.CancelledError as ex:
        pass
    finally:
        await shutdown.shutdown()
        pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except asyncio.CancelledError as ex:
        pass

