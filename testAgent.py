#coding:utf8

from sanic import Sanic
import os
import copy
import json as jsonlib
import time
import asyncio
from sanic.response import json,text
from functools import partial
import aiofiles
import logging
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler
import traceback

app = Sanic("testAgent")

saved_result = {}

@app.route("/",methods=["GET"])
async def index(request):
    return json({'msg':"Hello"})

@app.route('/run_stable_client',methods=['POST'])
async def run_stable_client(request):
    code = request.json.get('code','').strip()
    params = request.json.get('params','').strip()
    work_path = request.json.get('work_path','').strip()
    if code and work_path:
        app.loop.create_task(run_stable_script(work_path, code, params))
        # return text('ok')
        return json({'status':'ok'})
    else:
        # return text('error: need code/work_path.')
        return json({'status':'error','msg':'need code/work_path'})

async def run_stable_script(work_path, code, params):
    try:
        proc1 = await asyncio.create_subprocess_shell('cd {0} && {1} &'.format(work_path, code + ' ' + params), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        stdout, stderr = await proc1.communicate()
        logger.info('run stable script finish, code: {0}.'.format(code))
    except Exception as e:
        logger.error('run stable script error: {0}'.format(traceback.format_exc()))

@app.route('/runclient', methods=['POST'])
async def runclient(request):
    request_id = request.json.get('request_id','')
    if request_id != '' and request_id not in saved_result:
        saved_result[request_id] = {}
    else:
        logger.info('request_id: {0} already exist or value is None.'.format(request_id))
        # return text('request_id: {0} already exist or value is None.'.format(request_id))
        return json({'status':'error','msg':'request_id already exist or value is None'})
    args = await check_json_data(request)
    if isinstance(args,str) and 'error' in args:
        saved_result.pop(request_id)
        # return text(args)
        return json({'status':'error','msg':args})
    code = args.get('code','').strip()
    result_file = args.get('result_file','').strip()
    params = args.get('params','').strip()
    work_path = args.get('work_path','').strip()
    # task_type = args.get('type','')
    # request_id = args.get('request_id','')

    try:
        saved_result[request_id]['data'] = None
        saved_result[request_id]['time'] = time.time()
        saved_result[request_id]['is_read'] = False
        saved_result[request_id]['status'] = 'running'
        task = app.loop.create_task(run_script(work_path, code, result_file, params, request_id))
        # task.add_done_callback(partial(callback, work_path, result_file, request_id))
        logger.info('add task success. request_id: {0}'.format(request_id))
        # return text('ok')
        return json({'status':'ok'})
    except Exception as e:
        saved_result[request_id]['status'] = 'finish'
        logger.error('add task error. request_id: {0}'.format(request_id))
        # return text('error')
        return json({'status':'error','msg':'add task error'})

async def check_json_data(request):
    args = {}
    need_keys = ('code','result_file','params','work_path','request_id')
    receive_data = request.json
    if receive_data:
        for k in need_keys:
            if k in request.json.keys():
                args[k] = request.json.get(k)
                logger.info('receive key: {0}, value: {1}'.format(k, receive_data.get(k)))
            else:
                logger.info('need {0}'.format(k))
                return 'error : need {0}'.format(k)
        return args
    return 'error : need json datas'

async def callback(work_path, result_file, request_id):
    try:
        if os.path.exists(os.path.join(work_path, result_file)):
            async with aiofiles.open(os.path.join(work_path, result_file), 'r') as f:
                data = await f.read()
                logger.info('get result_msg: {0}, request_id: {1}'.format(data, request_id))
                saved_result[request_id]['data'] = data
                saved_result[request_id]['time'] = time.time()
            logger.info('callback success. request_id: {0}'.format(request_id))
        else:
            logger.error('result file not exists . request_id: {0}'.format(request_id))
    except Exception as e:
        logger.error('callback error, request_id: {0}.'.format(request_id))

async def run_script(work_path, code, result_file, params, request_id):
    try:
        proc0 = await asyncio.create_subprocess_shell(':> {0}'.format(os.path.join(work_path, result_file)), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        stdout0, stderr0 = await proc0.communicate()
        logger.info('clear result_file: {0} finish.'.format(result_file))

        proc1 = await asyncio.create_subprocess_shell('cd {0} && {1}'.format(work_path, code + ' ' + params + ' ' + result_file), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        stdout, stderr = await proc1.communicate()
        saved_result[request_id]['status'] = 'finish'
        logger.info('run client script finish, request_id: {0}.'.format(request_id))
        await callback(work_path, result_file, request_id)
    except Exception as e:
        saved_result[request_id]['status'] = 'finish'
        logger.error('create_subprocess_shell error: {0}'.format(traceback.format_exc()))

async def init_saved_result_datas():
    global saved_result
    if not saved_result and os.path.exists('saved_result_dump.json'):
        async with aiofiles.open('saved_result_dump.json','r') as f:
            data = await f.read()
            saved_result =  jsonlib.loads(data)

@app.route('/select_result',methods=['GET'])
async def select_result(request):
    request_id = request.args.get('request_id','').strip()
    if request_id in saved_result:
        if saved_result.get(request_id).get('status') == 'finish':
            saved_result[request_id]['is_read'] = True
        resp_json = {}
        resp_json[request_id] = copy.deepcopy(saved_result.get(request_id))
        if 'is_read' in resp_json[request_id]:
            resp_json[request_id].pop('is_read')
        if 'time' in resp_json[request_id]:
            resp_json[request_id].pop('time')
        logger.info('return get_request response: {0}, request_id: {1}'.format(resp_json,request_id))
        return json(resp_json)
    else:
        # return text('request_id: {0} result not exist.'.format(request_id))
        return json({'status':'error','msg':'request_id result not exist'})

def test_log():
    log_fmt = '%(asctime)s\t%(threadName)s,line %(lineno)s, %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_fmt)
    log_file_handler = TimedRotatingFileHandler(filename="./log/testagent.log", when="midnight", interval=1, backupCount=3)
    log_file_handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger()
    log.addHandler(log_file_handler)
    return log

file_tell = {}

@app.route('/get_log',methods=['GET'])
async def get_log(request):
    request_id = request.args.get('request_id','')
    if not request_id.strip():
        # return text('error: request_id can not null.')
        return json({'status':'error','msg':'request_id can not null'})
    log_file = request.args.get('log_file','')
    keyword = request.args.get('keyword','')
    if request_id not in file_tell and log_file.strip() and keyword.strip():
        file_tell[request_id] = {}
        file_tell[request_id]['log_file'] = log_file
        file_tell[request_id]['update'] = False
        file_tell[request_id]['offset'] = 0
    elif request_id in file_tell and file_tell[request_id].get('log_file','') and log_file != file_tell[request_id].get('log_file'):
        # return text('error: request_id and another log_file exists.')
        return json({'status':'error','msg':'request_id and another log_file exists'})
    elif not log_file or not keyword:
        # return text('error: log_file and keyword can not null.')
        return json({'status':'error','msg':'log_file and keyword can not null'})
    overtime = 60
    num = 0
    file_tell[request_id]['update'] = False
    file_tell[request_id]['overtime'] = False
    f_tell = file_tell.get(request_id).get('offset')
    start_time = time.time()
    async with aiofiles.open(log_file,'r+',encoding='utf-8') as f:
        await f.seek(0,2)
        end_tell = await f.tell()
        if end_tell < f_tell:
            await f.seek(0,0)
        else:
            await f.seek(f_tell,0)
        while True:
            line = await f.readline()
            if len(line) == 0:
                offset = await f.tell()
                if offset != file_tell[request_id]['offset']:
                    file_tell[request_id]['offset'] = offset
                    file_tell[request_id]['update'] = True
                break
            if keyword in line:
                num += 1
            if time.time() - start_time >= overtime:
                await f.seek(0,2)
                file_tell[request_id]['offset'] = await f.tell()
                file_tell[request_id]['update'] = True
                file_tell[request_id]['overtime'] = True
                logger.info('read log file overtime. request_id: {0}'.format(request_id))
                break
    logger.info('return num: {0}, request_id: {1}, current tell :{2}'.format(str(num), request_id,file_tell[request_id]))
    # return text(str(num))
    return json({'num':num,'is_update':file_tell[request_id].get('update'),'overtime':file_tell[request_id].get('overtime')})

async def clear_saved_data():
    await init_saved_result_datas()
    while True:
        try:
            if len(saved_result) > 100:
                overtime = 60 * 60 * 24
                run_over_time = overtime
                now_time = time.time()
                need_removes = []
                for k,v in saved_result.items():
                    if v.get('status','') == 'finish':
                        if now_time - v.get('time') >= overtime and v.get('is_read'):
                            need_removes.append(k)
                    elif v.get('status','') == 'running':
                        if now_time - v.get('time') >= run_over_time:
                            need_removes.append(k)
                for key in need_removes:
                    temp = saved_result.pop(key)
                    logger.info('remove overtime: key: {0}, value: {1}'.format(key,temp))
            await asyncio.sleep(60 * 20)
            if len(saved_result) != 0:
                async with aiofiles.open('./saved_result_dump.json','w') as f:
                    await f.write(jsonlib.dumps(saved_result))
            logger.info('clear overtime datas success.')
        except Exception as e:
            logger.error('clear_saved_result error , {0}'.format(traceback.format_exc()))

app.add_task(clear_saved_data)

app.config.REQUEST_TIMEOUT = 300
app.config.RESPONSE_TIMEOUT = 300

logger = test_log()

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8080)