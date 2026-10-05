import asyncio
import logging
import traceback
import json
import os
from signal import SIGINT, SIGTERM
from quart import  render_template,request,stream_with_context,redirect,make_response
from datetime import datetime,timedelta
import httpx

from status import app,application
from . import settings
from .healthcheckclient import healthstatuslistener,editinghealthstatuslistener
from .healthcheck import healthcheck,LastHealthCheck,SystemViewMeta,PRTGSensorMeta
from .socket import commandclient
from . import shutdown
from . import serializers
from . import utils


logger = logging.getLogger("healthcheck.healthcheckclient")

_permissions = {}
async def can_admin(request):
    host = request.headers.get("host")
    if any(host.startswith(k) for k in  ("localhost:","127.0.0.1:")):
        return settings.DEBUG

    user = request.headers.get("X-email")
    if not user:
        return False
    try:
        perm = _permissions.get(user)
        now = utils.now()
        if not perm or (now - perm[1]).total_seconds() > settings.AUTH2_PERMCACHE_TIMEOUT:
            res = None
            async with httpx.AsyncClient(auth=(settings.AUTH2_USER,settings.AUTH2_PASSWORD),timeout=settings.AUTH2_TIMEOUT,verify=settings.AUTH2_SSLVERIFY) as client:
                res = await client.post("{}/sso/checkauthorization".format(settings.AUTH2_URL),data={"details":"false","flaturl":"true","flatuser":"true","url":"https://{}/healthcheck/config".format(host),"user":user})
            data = res.json()
            perm = (data[-1],now)
            _permissions[user] = perm
        return perm[0]
    except Exception as ex:
        logger.error("Failed to get the permission from auth2.{}: {}".format(ex.__class__.__name__,str(ex)))
        return False

def exithandler():
    shutdown.shutdowning = True
    healthstatuslistener.request2close()
    editinghealthstatuslistener.request2close()

@app.before_serving
async def initialize():
    loop = asyncio.get_running_loop()
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, exithandler)
    healthstatuslistener.start()
    editinghealthstatuslistener.start()

@app.after_serving
async def post_shutdown():
    await shutdown.shutdown()

async def ping():
    start = utils.now()
    nextcheck = start + timedelta(seconds = settings.HEARTBEAT) 
    try:
        res = await commandclient.exec("healthcheck",1) 
        msg = res[1]
        status = "green" if res[0] else "red"
    except Exception as ex:
        status = "red"
        msg = "Failed to ping healthcheck server. {}:{}".format(ex.__class__.__name__,str(ex))
    finally:
        end = utils.now()
    return "{}\n".format(json.dumps([["healthcheck","healthcheck"],[nextcheck,[start,end,status,msg,False ]]],cls=serializers.JSONFormater)).encode()

@app.route("/healthcheck/ping")
async def healthcheck_ping():
    try:
        res = await commandclient.exec("healthcheck",1) 
        msg = res[1]
        status = 200 if res[0] else 500
    except Exception as ex:
        status = 500
        msg = "{}:{}".format(ex.__class__.__name__,str(ex))
    return msg ,status

@app.route("/healthcheck")
@app.route("/healthcheck/")
async def healthcheckindex():
    user = request.headers.get("X-email")
    if user:
        defaultview = healthcheck.get_viewmeta(user)
    else:
        defaultview = {
            "id":"Default",
            "title": healthcheck.title,
            "description":"The dashboard for all systems"
        }

    adminable = await can_admin(request)

    return await render_template("healthcheck/index.html",systemviews=healthcheck.systemviews,can_admin=adminable,defaultview=defaultview,prtgsensors=healthcheck.prtgsensors)


@app.route("/healthcheck/systemview",methods=["GET","POST"],defaults={'systemid':None})
@app.route("/healthcheck/systemview/<systemid>",methods=["GET","POST"])
async def save_systemview(systemid):
    #permission check
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    if request.method == "GET":
        if systemid:
            editing = True
            viewmeta = healthcheck.get_viewmeta(systemid)
        else:
            editing = False
            viewmeta = SystemViewMeta(["","","",""])
        return await render_template("healthcheck/systemview.html",viewmeta=viewmeta,editing = editing)
    elif request.method == "POST":
        formdata = await request.form
        action = formdata.get("action","save")
        if action == "save":
            messages = []
            if systemid:
                editing = True
            else:
                editing = False
                systemid = formdata.get("id")
                if healthcheck.get_viewmeta(systemid):
                    messages.append("System view({}) already exists".format(systemid))

            title = formdata.get("title")
            description = formdata.get("description")
            if not systemid :
                messages.append("System Identity can't be empty")
            elif " " in systemid :
                messages.append("System Identity can't contain space")
            elif "@" in systemid:
                messages.append("System Identity can't contain '@'")
            if not title:
                messages.append("Title can't be empty.")
            if messages:
                viewmeta = SystemViewMeta([systemid,title,description])
                return await render_template("healthcheck/systemview.html",viewmeta=viewmeta,messages=messages,editing=editing)
            healthcheck.save_systemview(systemid,title,description)

            return redirect('/healthcheck')
        elif action == "cancel":
            return redirect("/healthcheck")
        else:
            raise Exception("Action({}) Not Support".format(action))


@app.route("/healthcheck/prtgsensor",methods=["GET","POST"],defaults={'sensorid':None})
@app.route("/healthcheck/prtgsensor/<sensorid>",methods=["GET","POST"])
async def save_prtgsensor(sensorid):
    #permission check
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    if request.method == "GET":
        if sensorid:
            editing = True
            sensormeta = healthcheck.get_prtgsensormeta(sensorid)
        else:
            editing = False
            sensormeta = PRTGSensorMeta(["","","",""])
        return await render_template("healthcheck/prtgsensor.html",sensormeta=sensormeta,editing=editing)
    elif request.method == "POST":
        formdata = await request.form
        action = formdata.get("action","save")
        if action == "save":
            messages = []
            if sensorid:
                editing = True
            else:
                editing = False
                sensorid = formdata.get("id")
                if healthcheck.get_prtgsensormeta(sensorid):
                    messages.append("PRTG Sensor({}) already exists".format(sensorid))

            title = formdata.get("title")
            description = formdata.get("description")
            if not sensorid :
                messages.append("PRTG Sensor Identity can't be empty")
            elif " " in sensorid :
                messages.append("PRTG Sensor Identity can't contain space")
            if not title:
                messages.append("Title can't be empty.")
            if messages:
                sensormeta = PRTGSensorMeta([sensorid,title,description])
                return await render_template("healthcheck/prtgsensor.html",sensormeta=sensormeta,messages=messages,editing=editing)
            healthcheck.save_prtgsensor(sensorid,title,description)

            return redirect('/healthcheck')
        elif action == "cancel":
            return redirect("/healthcheck")
        else:
            raise Exception("Action({}) Not Support".format(action))


@app.route("/healthcheck/systemview/<systemid>/delete",methods=["GET"])
async def delete_systemview(systemid):
    healthcheck.delete_systemview(systemid)
    return redirect('/healthcheck')

@app.route("/healthcheck/prtgsensor/<sensorid>/delete",methods=["GET"])
async def delete_prtgsensor(sensorid):
    healthcheck.delete_prtgsensor(sensorid)
    return redirect('/healthcheck')

@app.route("/healthcheck/dashboard",defaults={'system': None})
@app.route("/healthcheck/dashboard/<system>")
async def dashboard(system):
    debug = request.args.get("debug","false").lower() == "true"
    healthservice_nextcheck = utils.now() + timedelta(seconds=settings.HEARTBEAT + 1)
    healthservice_nextcheck = int(healthservice_nextcheck.timestamp()) * 1000
    user = request.headers.get("X-email")
    adminable = await can_admin(request)
    if system:
        viewkey = system
        statusstreamurl = "/healthcheck/healthstatusstream/{}".format(system)
    else:
        viewkey = user
        statusstreamurl = "/healthcheck/healthstatusstream"

    if debug:
        statusstreamurl = "{}?debug=true".format(statusstreamurl)


    if viewkey:
        healthcheckview = healthcheck.get_view(viewkey)
    else:
        healthcheckview = healthcheck


    return await render_template("healthcheck/dashboard.html",healthcheck=healthcheckview,healthservice_nextcheck=healthservice_nextcheck,nextcheck_timeout_delay=settings.NEXTCHECK_TIMEOUT_DELAY,nextcheck_checkinterval=settings.NEXTCHECK_CHECKINTERVAL,baseurl="/healthcheck",statusstreamurl=statusstreamurl,adminable=adminable,heartbeat=settings.HEARTBEAT,user=user,system=system,debug=debug)

@app.route("/healthcheck/reload")
async def reload_dashboard():
    try:
        res = await commandclient.exec("reload_dashboard",1) 
        return "OK", 200
    except Exception as ex:
        return "Failed to reload the dashboard.{}".format(str(ex)), 500

def dump_servicehealthstatus(debug,sectionid,serviceid,healthstatus):
    if debug:
        return '[[\"{}\",\"{}\"],[{},[{},{},\"{}\",{},{}]],\"{}\"]\n'.format(
            sectionid,
            serviceid,
            healthstatus[0].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[0] else "null",
            healthstatus[1][0].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[1] and healthstatus[1][0] else "null",
            healthstatus[1][1].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[1] and healthstatus[1][1] else "null",
            healthstatus[1][2] if healthstatus[1] else "" ,
            json.dumps(healthstatus[1][3] if healthstatus[1] else ""),
            "true" if healthstatus[1] and healthstatus[1][-1] else "false",
            datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
        )
    else:
        return '[[\"{}\",\"{}\"],[{},[{},{},\"{}\",{},{}]]]\n'.format(
            sectionid,
            serviceid,
            healthstatus[0].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[0] else "null",
            healthstatus[1][0].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[1] and healthstatus[1][0] else "null",
            healthstatus[1][1].strftime("\"%Y-%m-%dT%H:%M:%S.%f\"") if healthstatus[1] and healthstatus[1][1] else "null",
            healthstatus[1][2] if healthstatus[1] else "" ,
            json.dumps(healthstatus[1][3] if healthstatus[1] else ""),
            "true" if healthstatus[1] and healthstatus[1][-1] else "false"
        )

@app.route("/healthcheck/healthstatusstream",defaults={'system': None})
@app.route("/healthcheck/healthstatusstream/<system>")
async def healthstatusstream(system):
    viewkey = system or request.headers.get("X-email")
    viewsettings = healthcheck.get_viewsettings(viewkey)
    debug = request.args.get("debug","false").lower() == "true"

    @stream_with_context
    async def async_generator():
        for section in healthcheck.healthchecksections:
            for service in section.healthcheckservices:
                if service.healthstatus:
                    yield dump_servicehealthstatus(debug,section.sectionid,service.serviceid,service.healthstatus).encode()

        reader = healthstatuslistener.get_healthstatusreader()
        while not shutdown.shutdowning:
            await healthstatuslistener.wait(settings.STATUSSTREAM_HEARTBEAT)
            sent_heartbeat = True
            for healthstatus in reader.items():
                if isinstance(healthstatus,str):
                    yield "{}\n".format(json.dumps(healthstatus,cls=serializers.JSONFormater)).encode()
                else:
                    yield dump_servicehealthstatus(debug,*healthstatus[0],healthstatus[1]).encode()
                sent_heartbeat = False
            if sent_heartbeat:
                yield b"\n"


    @stream_with_context
    async def async_generator_view():
        for section in healthcheck.healthchecksections:
            serviceset = viewsettings.get(section.sectionid,set())
            for service in section.healthcheckservices:
                if service.healthstatus and service.serviceid in serviceset:
                    yield dump_servicehealthstatus(debug,section.sectionid,service.serviceid,service.healthstatus).encode()

        reader = healthstatuslistener.get_healthstatusreader()
        while not shutdown.shutdowning:
            try:
                async with asyncio.timeout(settings.STATUSSTREAM_HEARTBEAT):
                    await healthstatuslistener.wait()
                for healthstatus in reader.items():
                    if isinstance(healthstatus,str):
                        yield "{}\n".format(json.dumps(healthstatus,cls=serializers.JSONFormater)).encode()
                    elif healthstatus[0][1] in viewsettings.get(healthstatus[0][0],set()):
                        yield dump_servicehealthstatus(debug,*healthstatus[0],healthstatus[1]).encode()
            except asyncio.TimeoutError as ex:
                yield b"\n"

    response = await make_response(
        async_generator_view() if viewsettings else async_generator(),
        {
            # The Cache-Control header need to be set thus to work behind Fastly caching.
            "Content-Type": "text/event-stream",
            "Cache-Control": "private, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable response buffering in Nginx.
            "x-content-type-options":"nosniff",
            "x-frame-options": "DENY"
        }
    )
    response.timeout = None  # Prevents Quart from killing the connection

    return response

@app.route("/healthcheck/customize",defaults={'system': None},methods=["GET","POST"])
@app.route("/healthcheck/config/view/<system>",methods=["GET","POST"])
async def customize_dashboard(system):
    #permission check
    if system:
        editable = await can_admin(request)
        if not editable:
            return "Not Authorized", 403


    user = request.headers.get("X-email")
    viewkey = system or user
    if request.method == "GET":
        healthcheckview = healthcheck.get_view(viewkey)

        return await render_template("healthcheck/customize.html",healthcheck=healthcheckview,viewkey=viewkey,system=system,user=user)
    else:
        try:
            formdata = await request.form
            action = formdata.get("action","save")
            if action == "save":
                all_selected = True
                viewsettings = {}

                for section in healthcheck.healthchecksections:
                    for service in section.healthcheckservices:
                        if "{}:{}".format(section.sectionid,service.serviceid) in formdata:
                            if section.sectionid not in viewsettings:
                                viewsettings[section.sectionid] = set()
                            viewsettings[section.sectionid].add(service.serviceid)
                        else:
                            all_selected = False

                if all_selected or not viewsettings:
                    healthcheck.save_viewsettings(viewkey)
                else:
                    healthcheck.save_viewsettings(viewkey,viewsettings)
            elif action == "reset":
                healthcheck.save_viewsettings(viewkey)
            else:
                raise Exception("Action({}) Not Support".format(action))

            msg = None
        except Exception as ex:
            traceback.print_exc()
            msg = str(ex)
        if system:
            return redirect("/healthcheck/dashboard/{}".format(system))
        else:
            return redirect("/healthcheck/dashboard")


@app.route("/healthcheck/config/prtgsensor/<sensorid>",methods=["GET","POST"])
async def customize_prtgsensor(sensorid):
    #permission check
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403


    if request.method == "GET":
        healthcheckview = healthcheck.get_prtgsensor(sensorid)

        return await render_template("healthcheck/config_prtgsensor.html",healthcheck=healthcheckview,sensorid=sensorid)
    else:
        try:
            formdata = await request.form
            action = formdata.get("action","save")
            if action == "save":
                viewsettings = {}

                for section in healthcheck.healthchecksections:
                    if not section.prtgenabled:
                        continue
                    for service in section.healthcheckservices:
                        if not service.prtgenabled:
                            continue
                        for channelid,prtgconfig in service.prtgchannels:
                            if "{}:{}:{}".format(section.sectionid,service.serviceid,channelid) in formdata:
                                if section.sectionid not in viewsettings:
                                    viewsettings[section.sectionid] = {}
                                if service.serviceid not in viewsettings[section.sectionid]:
                                    viewsettings[section.sectionid][service.serviceid] = []
                                viewsettings[section.sectionid][service.serviceid].append(channelid)

                healthcheck.save_prtgsensorsettings(sensorid,viewsettings)
            elif action == "reset":
                healthcheck.save_prtgsensorsettings(sensorid)
            else:
                raise Exception("Action({}) Not Support".format(action))

            msg = None
        except Exception as ex:
            traceback.print_exc()
            msg = str(ex)
        return redirect("/healthcheck")

@app.route("/healthcheck/history/<sectionid>/<serviceid>",defaults={'pageid': ""})
@app.route("/healthcheck/history/<sectionid>/<serviceid>/<pageid>")
async def healthcheckhistory(sectionid,serviceid,pageid):
    service = healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    pages = service.healthcheckpages.get_pages()

    if pageid:
        try:
            pageid = int(pageid)
        except:
            return redirect("/healthcheck/history/{}/{}".format(sectionid,serviceid))

        page = next((p for p in pages if pageid == p.pageid),None)
        if not page:
            return redirect("/healthcheck/history/{}/{}".format(sectionid,serviceid))
    elif pages:
        page = pages[-1]
    else:
        page = None

    if page and not os.path.exists(page.filepath):
        #file doesn't exit, maybe deleted from disk. reload the pages 
        service.healthcheckpages.reset()
        return redirect("/healthcheck/history/{}/{}".format(sectionid,serviceid))

    return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck",history="history",title="Health Check Histories")

@app.route("/healthcheck/errorhistory/<sectionid>/<serviceid>",defaults={'pageid': ""})
@app.route("/healthcheck/errorhistory/<sectionid>/<serviceid>/<pageid>")
async def healthcheckerrorhistory(sectionid,serviceid,pageid):
    service = healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    errorpages = service.healthcheckpages.errorpages
    if not errorpages:
        return "The error history of the service({}.{}) is not enabled.".format(sectionid,serviceid) ,404

    pages = errorpages.get_pages()

    if pageid:
        try:
            pageid = int(pageid)
        except:
            return redirect("/healthcheck/errorhistory/{}/{}".format(sectionid,serviceid))

        page = next((p for p in pages if pageid == p.pageid),None)
        if not page:
            return redirect("/healthcheck/errorhistory/{}/{}".format(sectionid,serviceid))
    elif pages:
        page = pages[-1]
    else:
        page = None

    if page and not os.path.exists(page.filepath):
        #file doesn't exit, maybe deleted from disk. reload the pages 
        service.healthcheckpages.reset()
        return redirect("/healthcheck/errorhistory/{}/{}".format(sectionid,serviceid))

    return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck",history="errorhistory",title="Health Check Error Histories")

@app.route("/healthcheck/details/<sectionid>/<serviceid>/<starttime>")
async def healthcheckdetails(sectionid,serviceid,starttime):
    service = healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    starttime = datetime.strptime(starttime,'%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=settings.TZ)

    try:
        detailfile = service.healthcheckpages.detailfile(starttime)
        with open(detailfile) as f:
            data = f.read()
        return data,200,{"Content-Type":"application/json"}
    except Exception as ex:
        return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck",history="history",message=str(ex))

@app.route("/healthcheck/config/edit",methods=["GET","POST"])
async def edit_healthcheck():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    if request.method == "GET":
        with open(healthcheck.editconfigfile) as f:
            healthcheckconfig = f.read()
            return await render_template("healthcheck/edit.html",healthcheckconfig=healthcheckconfig,message=None)
    else:
        try:
            formdata = await request.form
            healthcheckconfig = ""
            action = formdata.get("action","save")
            if action == "save":
                healthcheckconfig = formdata.get("healthcheckconfig")
                changed = healthcheck.editing_healthcheck.save(healthcheckconfig)
            elif action == "reset":
                changed = healthcheck.editing_healthcheck.reset()
                with open(healthcheck.editing_healthcheck.configfile) as f:
                    healthcheckconfig = f.read()
            else:
                raise Exception("Action({}) Not Support".format(action))

            if changed:
                await commandclient.exec("reload_editing_healthcheck",1) 

            msg = None
        except Exception as ex:
            traceback.print_exc()
            msg = str(ex)

        return await render_template("healthcheck/edit.html",healthcheckconfig=healthcheckconfig,message=msg)

@app.route("/healthcheck/config/publish",methods=["GET","POST"])
async def publish_healthcheck():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    if request.method == "GET":
        with open(healthcheck.editconfigfile) as f:
            healthcheckconfig = f.read()
            return await render_template("healthcheck/publish.html",healthcheckconfig=healthcheckconfig,message=None)
    else:
        try:
            formdata = await request.form
            action = formdata.get("action","publish")
            if action == "publish":
                comments = formdata.get("comments")
                if not comments:
                    comments = "No comments"
                user = request.headers.get("x-email")
                if not user:
                    user = "guest"
                changed = healthcheck.editing_healthcheck.publish(user,comments)
            else:
                raise Exception("Action({}) Not Support".format(action))

            if changed:
                await commandclient.exec("reload_healthcheck",1) 

            return redirect("/healthcheck/config/publishhistories")

        except Exception as ex:
            traceback.print_exc()
            msg = str(ex)
            return await render_template("healthcheck/edit.html",healthcheckconfig=healthcheckconfig,message=msg)

@app.route("/healthcheck/config/publishhistories",methods=["GET"])
async def publishhistories():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    return await render_template("healthcheck/publishhistories.html",publishhistories=healthcheck.publishhistories,message=None)

@app.route("/healthcheck/config/rollback",methods=["POST"])
async def rollback():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    try:
        formdata = await request.form
        configfile = formdata.get("configfile")
        if not configfile:
            raise Exception( "Missing configfile.")
        else:
            changed = healthcheck.rollback(configfile)

        if changed:
            await commandclient.exec("reload_healthcheck",1) 

        return redirect("/healthcheck/dashboard")
    except Exception as ex:
        traceback.print_exc()
        msg = str(ex)
        return await render_template("healthcheck/publishhistories.html",publishhistories=healthcheck.publishhistories,message=msg)



@app.route("/healthcheck/config/preview",methods=["GET"])
async def preview_editing_healthcheck():
    debug = request.args.get("debug","false").lower() == "true"
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    try:
        result = await commandclient.exec("start_preview_healthcheck",1)
        if result[0]:
            editinghealthstatuslistener.continuouscheck_started = True
            msg = None
        else:
            msg = [result[1]]
    except Exception as ex:
        traceback.print_exc()
        msg = [str(ex)]

    if debug:
        statusstreamurl = "/healthcheck/config/healthstatusstream?debug=true"
    else:
        statusstreamurl = "/healthcheck/config/healthstatusstream"


    healthservice_nextcheck = utils.now() + timedelta(seconds=settings.HEARTBEAT + 1)
    healthservice_nextcheck = int(healthservice_nextcheck.timestamp()) * 1000
    return await render_template("healthcheck/preview.html",healthcheck=healthcheck.editing_healthcheck,messages=msg,healthservice_nextcheck=healthservice_nextcheck,nextcheck_timeout_delay=settings.NEXTCHECK_TIMEOUT_DELAY,nextcheck_checkinterval=settings.NEXTCHECK_CHECKINTERVAL,baseurl="/healthcheck/config",statusstreamurl=statusstreamurl,heartbeat=settings.HEARTBEAT,debug=debug)

@app.route("/healthcheck/config/history/<sectionid>/<serviceid>",defaults={'pageid': ""})
@app.route("/healthcheck/config/history/<sectionid>/<serviceid>/<pageid>")
async def editinghealthcheckhistory(sectionid,serviceid,pageid):
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    service = healthcheck.editing_healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    pages = service.healthcheckpages.get_pages()
    if pageid:
        try:
            pageid = int(pageid)
        except:
            return redirect("/healthcheck/config/history/{}/{}".format(sectionid,serviceid))
        page = next((p for p in pages if pageid == p.pageid),None)
        if not page:
            return redirect("/healthcheck/config/history/{}/{}".format(sectionid,serviceid))
    elif pages:
        page = pages[-1]
    else:
        page = None

    if page and not os.path.exists(page.filepath):
        #file doesn't exit, maybe deleted from disk. reload the pages 
        service.healthcheckpages.reset()
        return redirect("/healthcheck/config/history/{}/{}".format(sectionid,serviceid))

    return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck/config",history="history")

@app.route("/healthcheck/config/errorhistory/<sectionid>/<serviceid>",defaults={'pageid': ""})
@app.route("/healthcheck/config/errorhistory/<sectionid>/<serviceid>/<pageid>")
async def editinghealthcheckerrorhistory(sectionid,serviceid,pageid):
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    service = healthcheck.editing_healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    errorpages = service.healthcheckpages.errorpages
    if not errorpages:
        return "The error history of the service({}.{}) is not enabled.".format(sectionid,serviceid) ,404

    pages = errorpages.get_pages()
    if pageid:
        try:
            pageid = int(pageid)
        except:
            return redirect("/healthcheck/config/errorhistory/{}/{}".format(sectionid,serviceid))
        page = next((p for p in pages if pageid == p.pageid),None)
        if not page:
            return redirect("/healthcheck/config/errorhistory/{}/{}".format(sectionid,serviceid))
    elif pages:
        page = pages[-1]
    else:
        page = None

    if page and not os.path.exists(page.filepath):
        #file doesn't exit, maybe deleted from disk. reload the pages 
        service.healthcheckpages.reset()
        return redirect("/healthcheck/config/errorhistory/{}/{}".format(sectionid,serviceid))

    return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck/config",history="errorhistory")

@app.route("/healthcheck/config/details/<sectionid>/<serviceid>/<starttime>")
async def editinghealthcheckdetails(sectionid,serviceid,starttime):
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    service = healthcheck.editing_healthcheck.get_service(sectionid,serviceid)
    if not service:
        return "The service({}.{}) doesn't exist".format(sectionid,serviceid) ,404

    starttime = datetime.strptime(starttime,'%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=settings.TZ)

    try:
        detailfile = service.healthcheckpages.detailfile(starttime)
        with open(detailfile) as f:
            data = f.read()
        return data,200,{"Content-Type":"application/json"}
    except Exception as ex:
        return await render_template("healthcheck/healthcheckhistory.html",service=service,pages=reversed(pages),page=page,baseurl="/healthcheck/config",history="history",message=str(ex))


@app.route("/healthcheck/config/preview/start",methods=["GET"])
async def start_preview_editing_healthcheck():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    try:
        result = await commandclient.exec("start_preview_healthcheck",1)
        if result[0]:
            editinghealthstatuslistener.continuouscheck_started = True
            msg = "OK"
        else:
            msg = result[1]
        return msg
    except Exception as ex:
        msg = "Failed to start editing health check preview. {}".format(str(ex))
        return msg,400

@app.route("/healthcheck/config/preview/stop",methods=["GET"])
async def stop_preview_editing_healthcheck():
    editable = await can_admin(request)
    if not editable:
        return "Not Authorized", 403

    try:
        result = await commandclient.exec("stop_preview_healthcheck",1)
        if result[0]:
            editinghealthstatuslistener.continuouscheck_started = False
            msg = "OK"
        else:
            msg = result[1]
        return msg
    except Exception as ex:
        msg = "Failed to stop editing health check preview. {}".format(str(ex))
        return msg,400


@app.route("/healthcheck/json",defaults={'system': None})
@app.route("/healthcheck/json/<system>")
def jsonstatus(system):
    viewkey = system or request.headers.get("X-email")
    details = request.args.get("details") or ""
    return healthcheck.get_view(viewkey).get_jsonstatus(details.lower() == "true"),200,{"Content-Type":"application/json"}

@app.route("/healthcheck/prtg/<sensorid>")
def prtg(sensorid):
    #frist to try prtg sensor
    try:
        return healthcheck.get_prtgsensor(sensorid).get_prtgdata(),200,{"Content-Type":"application/json"}
    except:
        #second to try health checkview
        if healthcheck.get_viewmeta(sensorid):
            healthcheckview = healthcheck.get_view(sensorid)
            return healthcheckview.get_prtgdata(),200,{"Content-Type":"application/json"}
    return "PRTG Sensor({}) Not Found".format(sensorid),404

@app.route("/healthcheck/config/healthstatusstream")
async def editinghealthstatusstream():
    editable = await can_admin(request)
    debug = request.args.get("debug","false").lower() == "true"
    if not editable:
        return "Not Authorized", 403

    @stream_with_context
    async def async_generator():
        for section in healthcheck.editing_healthcheck.healthchecksections:
            for service in section.healthcheckservices:
                yield dump_servicehealthstatus(debug,section.sectionid,service.serviceid,service.healthstatus).encode()

        if editinghealthstatuslistener.continuouscheck_started:
            yield "{}\n".format(json.dumps("continuouscheck_started")).encode()
        elif editinghealthstatuslistener.continuouscheck_started == False:
            yield "{}\n".format(json.dumps("continuouscheck_stopped")).encode()

        reader = editinghealthstatuslistener.get_healthstatusreader()
        while not shutdown.shutdowning:
            await editinghealthstatuslistener.wait(settings.STATUSSTREAM_HEARTBEAT)
            sent_heartbeat = True
            for healthstatus in reader.items():
                if isinstance(healthstatus,str):
                    yield "{}\n".format(json.dumps(healthstatus,cls=serializers.JSONFormater)).encode()
                else:
                    yield dump_servicehealthstatus(debug,*healthstatus[0],healthstatus[1]).encode()
                sent_heartbeat = False
            if sent_heartbeat:
                yield b"\n"

    response = await make_response(
        async_generator(),
        {
            # The Cache-Control header need to be set thus to work behind Fastly caching.
            "Content-Type": "text/event-stream",
            "Cache-Control": "private, no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable response buffering in Nginx.
            "x-content-type-options":"nosniff",
            "x-frame-options": "DENY"
        }
    )
    response.timeout = None  # Prevents Quart from killing the connection
    return response

if __name__ == "__main__":
    loop = shutdown.patch_asyncio()

    application.run(host="0.0.0.0", port=os.environ.get("PORT", 8080), use_reloader=True,loop=loop)
    print("**={}".format(application))
