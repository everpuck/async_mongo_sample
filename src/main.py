"""
    parser_sub_res.py

    Created by yuandongfang on 2018/01/10
    Copyright (c) 2018 piowind. All rights reserved
"""


import os
import json
# import aiofiles
import asyncio
from datetime import datetime
from zlib import decompress
from base64 import b64decode
from motor import motor_asyncio
from pymongo.errors import DuplicateKeyError

from mylog import setup_logger
import config

logger = setup_logger(logger_name='main', logger_path='log')

RET_SUCCESS = 200
DEFAULT_SUB_TYPE_NUM = 14
# SUFIX = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
# FAIL_FILE = "fail_file_" + SUFIX
# DONE_FILE = "done_file_" + SUFIX


def mongo_test():
     client = motor_asyncio.AsyncIOMotorClient(config.DB_URI)
     db = client['offline-data']
     return db

async def mongo_insert_test(data):
    db = mongo_test()
    # data={'test_key': 'test_value'}
    # data={'test_key': 'test_value', 'key': 1}

    try:
        result = await db[config.DB_PEOPLE_SUB_COL].insert_one(data)
    except DuplicateKeyError as e:
        print(e)
        print('catch duplicate key error')
        # print(type(result))
        # print(dir(result))
    # print('result %s' % repr(result.inserted_id))

async def mongo_update_one(db, col, key_dict, res, upsert=False):
    try:
        result = await db[col].update_one(
            key_dict,
            {'$set': res, '$currentDate': {'lastModified': True}},
            upsert=upsert
        )
    except Exception as e:
        logger.error('error in update one {}'.format(e))
        return 'error'
    else:
        return result.raw_result
    # print(result.matched_count, result.modified_count)
    # print(result.upserted_id)

async def mongo_find_and_modify(
    db, col, query_dict, update_dict={}, projection=None):
    try:
        # col = 'test_col'
        res = await db[col].find_one_and_update(
            query_dict,
            {'$set': update_dict},
            projection)
    except Exception as e:
        logger.error('error {} in find_and_modify operation '.format(e))
    else:
        return res

async def mongo_find_and_delete(db, col, query_dict, projection=None):
    try:
        res = await db[col].find_one_and_delete(
            query_dict, projection)
    except Exception as e:
        logger.error('error {} in find_and_delete operation '.format(e))
        return 'error'
    else:
        # print(repr(res))
        return res

async def mongo_find_one(db, col, query_dict, projection=None):
    try:
        res = await db[col].find_one(
            query_dict, None)
    except Exception as e:
        logger.error('error {} in find_and_one operation '.format(e))
        return 'error'
    else:
        # print(repr(res))
        return res

async def insert_one(db, col, data):
    try:
        result = await db[col].insert_one(data)
    except DuplicateKeyError as e:
        logger.warning('deplicate key error error in insert one {}'.format(e))
        return False
    except Exception as e:
        logger.warning('insert error error in insert one {}'.format(e))
        return False
    else:
        # print(repr(result.inserted_id))
        logger.info('insert one {}.'.format(str(result.inserted_id)))
        return True

async def insert_many(db, col, data_list, ordered=False):
    try:
        result = await db[col].insert_many(data_list, ordered=ordered)
    except Exception as e:
        # logger.error('error in insert many {}'.format(e))
        return False
    else:
        # print(dir(result))
        # print(repr(result.inserted_ids))
        logger.info('insert count {}.'.format(len(result.inserted_ids)))
        return True
        # return result.raw_result


async def save_to_file(line_list, path="fail_data"):
    filename = os.path.join(
        fail_data, "fail_data"+datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
    async with aiofiles.open(filename, 'a') as f:
        for line in line_list:
            if isinstance(line, dict):
                if '_id' in line:
                    line['_id'] = str(line['_id'])
                line = json.dumps(line)
                await f.write(line + '\n')
                await f.flush()


async def consumer(db, consumer_index):
    ''' worker for crawl '''
    save_list = []
    count = 5
    while True:
        # document = await mongo_find_one(
        #     db, config.DB_PEOPLE_COL, {}, {'crawl_fbid': 1})
        document = await mongo_find_and_modify(
            db, config.DB_PEOPLE_LAST_COL, {'status': 0},
            {'status': -1}, {'crawl_fbid': 1, 'task_batch': 1})
            # {'status': 11}, {'crawl_fbid': 1, 'sub_type_num': })
        if document is None:
            count -= 1
            if count < 0:
                break
            continue 
        else:
            count = 5

        task_batch = document.get('task_batch')
        if not task_batch:
            continue

        fbid = int(document.get('crawl_fbid'))
        document_oid = document.get('_id')
	
        logger.info(
            'get document oid: {0}, fbid: {1} from mongo'.format(
                str(document_oid), fbid))
        # sub_type_num = decument.get('sub_type_num') 
        sub_type_num = DEFAULT_SUB_TYPE_NUM 
        update_dict = {'status': 1}
        result_json = {}

        sub_type_count = 0
        async for sub_res in db[config.DB_PEOPLE_SUB_COL].find(
            {'crawl_fbid': fbid}):
            # {'crawl_fbid': {'$in':[fbid, str(fbid)]}}):
            sub_result_json = sub_res.get('result_json')
            sub_key = sub_res.get('sub_type')

            res_status = sub_res.get('status', -1)
            sub_type_count += 1
            if res_status != RET_SUCCESS and res_status != 402\
                and res_status != 403:
                update_dict.update({'status': -1})
                logger.warning(
                    'sub type: {0} error status: {1} continue'.format(
                        sub_key, res_status))
                result_json = {}
                break
            result_json.update({
                sub_key: sub_result_json.get(sub_key)  
            })

        # result_json is empty then do nothing and continue
        if not result_json:
            continue

        if sub_type_count < sub_type_num:
            update_dict.update({'status': 2})
            logger.warning('sub res count not enough, do nothon...')

        # result_json is valid
        query_dict2 = {'_id': document_oid}
        update_dict.update({
            'result_json': result_json
            })

        # test
        # with open("temp_update_res", 'w') as f:
        #     f.write(json.dumps(update_dict))

        for _ in range(3):
            ret = await mongo_update_one(
                db, config.DB_PEOPLE_LAST_COL, query_dict2, update_dict)
            if ret != 'error' and ret.get('nModified') == 1:
                is_updated = True 
                break
        if is_updated:
            # logger.info('finish update id {}'.format(fbid))
            logger.info('finish update id {0}'.format(fbid))
        else:
            logger.error('update id {} error'.format(fbid))
        
            # save_list.append(document)

            # save url to file
       #  if len(save_list) > 1000:
       #      await save_to_file(save_list)
       #      save_list = []
        # await asyncio.sleep(random.random())

        # await self.result_queue.put(None)
    # if len(save_list):
        # await save_to_file(save_list)

    print('consumer: {} done'.format(consumer_index))
    logger.info('consumer: {} done'.format(consumer_index))

def main():
    db = mongo_test()
    concurrency = 400
    # concurrency = 1
    # filepaths = ['data',]
    # tasks = [asyncio.ensure_future(get_files(filepaths, concurrency)),]

    tasks = []
    # tasks = [asyncio.ensure_future(get_line(filename)),]
    for i in range(concurrency):
        tasks.append(asyncio.ensure_future(consumer(db, i)))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    for task in tasks:
        print('Task ret: ', task.result())

    # loop.run_until_complete(mongo_find_and_delete(db, 'test_col', {},))
    # loop.run_until_complete(mongo_find_one(db, 'test_col', {'key': 3},))
    loop.close()


def main_test():
    # db = mongo_test()
    # concurrency = 20
    # concurrency = 1
    # filepaths = ['data',]
    # tasks = [asyncio.ensure_future(get_files(filepaths, concurrency)),]

    # tasks = []
    # tasks = [asyncio.ensure_future(get_line(filename)),]
    # for i in range(concurrency):
        # tasks.append(asyncio.ensure_future(consumer(db, i)))
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(asyncio.wait(tasks))
    # for task in tasks:
        # print('Task ret: ', task.result())

    # loop.run_until_complete(mongo_find_and_delete(db, 'test_col', {},))
    with open('temp', 'r') as f:
        info = eval(f.read())
        print(type(info))


    # loop.run_until_complete(mongo_insert_test(data))
    # loop.close()



if __name__ == "__main__":
    # print(mongo_test())
    # main_test()
    main()
