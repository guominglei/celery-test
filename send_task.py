#!/usr/local/env python
# -*- coding: utf-8 -*-

"""
    Author: GuoMingLei
    Created: 2019/3/29
    人为向redis发送消息。供celery worker 进行工作。
"""
import os
import uuid
import json
import redis
import base64
import socket

"""
    body = '[[30, 42], {}, {"chord": null, "callbacks": null, "errbacks": null, "chain": null}]'
    info = {
        u'body': u'W1szMCwgNDJdLCB7fSwgeyJjaG9yZCI6IG51bGwsICJjYWxsYmFja3MiOiBudWxsLCAiZXJyYmFja3MiOiBudWxsLCAiY2hhaW4iOiBudWxsfV0=', 
        u'headers': {
            u'origin': u'gen40789@mingleideMacBook-Pro.local', 
            u'lang': u'py', 
            u'task': u'tasks.add', 
            u'group': None, 
            u'root_id': u'e936afd7-f807-4214-800b-4142640c7457', 
            u'expires': None, 
            u'retries': 0, 
            u'timelimit': [None, None], 
            u'argsrepr': u'(30, 42)', 
            u'eta': None, 
            u'parent_id': None, 
            u'shadow': None, 
            u'id': u'e936afd7-f807-4214-800b-4142640c7457', 
            u'kwargsrepr': u'{}'
        }, 
        u'content-type': u'application/json', 
        u'properties': {
            u'body_encoding': u'base64', 
            u'delivery_info': {
                u'routing_key': u'celery', 
                u'exchange': u''
            }, 
            u'delivery_mode': 2, 
            u'priority': 0, 
            u'correlation_id': u'e936afd7-f807-4214-800b-4142640c7457', 
            u'reply_to': u'c6a4dacc-2c06-3548-965c-b3e3b22c5151', 
            u'delivery_tag': u'f8159c2e-8831-409e-a310-0c09ea1699d1'
        }, 
        u'content-encoding': u'utf-8'
    }
"""

ALI_SERVICE_NAME = os.environ.get('SERVICE_NAME')
ALI_INSTANCE_ID = os.environ.get('INSTANCE_ID')
if not ALI_SERVICE_NAME and not ALI_INSTANCE_ID:
    HOSTNAME = socket.gethostname()
else:
    HOSTNAME = "{}-{}".format(ALI_SERVICE_NAME, ALI_INSTANCE_ID)

PID = os.getpid()


def get_origin():
    """
        获取名称
    """
    origin = "gen{}@{}".format(os.getgid(), HOSTNAME)

    return origin


def create_body(args_list=[], args_dict={}):
    """
        生成base64编码的body
    """
    data = [list(args_list), args_dict, {"chord": None, "callbacks": None, "errbacks": None, "chain": None}]
    body_json = json.dumps(data)
    # print body_json
    body_encode = base64.b64encode(body_json)
    # print body_encode
    return body_encode


def create_msg(task_name, route_key, exchange="", arg_list=[], arg_dict={}):
    """
        创建消息实体
    """
    id = str(uuid.uuid4())
    reply_to = str(uuid.uuid4())
    delivery_tag = str(uuid.uuid4())
    body = create_body(args_list=arg_list, args_dict=arg_dict)
    msg = {
        u'body': body,
        u'headers': {
            u'origin': get_origin(),
            u'lang': u'py',
            u'task': task_name,
            u'group': None,
            u'root_id': id,
            u'expires': None,
            u'retries': 0,
            u'timelimit': [None, None],
            u'argsrepr': str(set(arg_list)),
            u'eta': None,
            u'parent_id': None,
            u'shadow': None,
            u'id': id,
            u'kwargsrepr': str(arg_dict)
        },
        u'content-type': u'application/json',
        u'properties': {
            u'body_encoding': u'base64',
            u'delivery_info': {
                u'routing_key': route_key,
                u'exchange': exchange,
            },
            u'delivery_mode': 2,
            u'priority': 0,
            u'correlation_id': id,
            u'reply_to': reply_to,
            u'delivery_tag': delivery_tag
        },
        u'content-encoding': u'utf-8'
    }

    msg_json = json.dumps(msg)

    return msg_json


def test():
    arg_list = [10, 20]
    arg_dict = {}

    task_name = "tasks.add"
    route_key = "celery"

    msg = create_msg(task_name, route_key, exchange="", arg_list=arg_list, arg_dict=arg_dict)
    print msg


def test_send():

    cf = "redis://127.0.0.1:6379/5"
    client = redis.Redis(connection_pool=redis.ConnectionPool.from_url(cf), socket_timeout=1)

    arg_list = [12, 20]
    arg_dict = {}

    task_name = "tasks.add"
    route_key = "celery"

    msg = create_msg(task_name, route_key, exchange="", arg_list=arg_list, arg_dict=arg_dict)
    print msg

    client.rpush(route_key, msg)


if __name__ == "__main__":

    #test()

    test_send()
