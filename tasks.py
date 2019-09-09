#!/usr/local/env python
# -*- coding: utf-8 -*-

"""
    Author: GuoMingLei
    Created: 2019/3/29
   
"""

from celery import Celery
app = Celery('tasks', broker='redis://127.0.0.1:6379/5')


@app.task(ignore_result=True)
def add(x, y):
    return x + y


if __name__ == '__main__':
    result = add.delay(30, 42)