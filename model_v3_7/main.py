"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 耳标状态和死亡判别模型
"""

import time
from datetime import datetime, timedelta

import pandas as pd
from DataSet import DataSet
from EarTagStateModel import EarTagStateModel
from DeathModel import DeathModel
from config import *
from param import *
from log import logger
from report import Report, send_error_report
from DataClean import DataClean
from DataCheck import DataCheck
import traceback
import argparse


def val(start_time, end_time=None, farm_id=None, label_number=None, report=False):
    print('Val time interval: %s---%s' % (start_time, end_time))
    time_pried = TIME_PRIED
    print('Back in the interval: %s' % time_pried)
    curDateTime = pd.to_datetime(start_time)
    times = 0
    backtrack_data = pd.DataFrame(None)
    while curDateTime < pd.to_datetime(end_time):
        backtrack_data = main(curDateTime, backtrack_data, farm_id, label_number, times, time_pried, curDateTime.minute,
                              report)
        curDateTime = curDateTime + timedelta(minutes=time_pried)
        times += 1


def recall(start_time, farm_id=None, label_number=None, report=True, times=0):
    print('回溯任务,时间: %s' % start_time)
    time_pried = TIME_PRIED
    curDateTime = pd.to_datetime(start_time)
    backtrack_data = pd.DataFrame(None)
    while curDateTime < datetime.now():
        backtrack_data = main(curDateTime, backtrack_data, farm_id, label_number, times, time_pried, curDateTime.minute,
                              report)
        curDateTime = curDateTime + timedelta(minutes=time_pried)
        times += 1
    print('结束回溯任务')
    return backtrack_data, times, curDateTime-timedelta(minutes=time_pried)


def real(farm_id=None, label_number=None):
    times = 0
    backtrack_data = pd.DataFrame(None)
    last_curDateTime = pd.to_datetime(datetime.now())
    while True:
        curDateTime = pd.to_datetime(datetime.now())
        time_pried = int((curDateTime - last_curDateTime).total_seconds() / 60)
        backtrack_data = main(curDateTime, backtrack_data, farm_id, label_number, times, time_pried, curDateTime.minute)
        times += 1
        last_curDateTime = curDateTime


def timing(farm_id=None, label_number=None, backtrack_data=pd.DataFrame(None), times=0, last_curDateTime=datetime.now()):
    print('定时任务，每%d分钟运行一次' % TIME_PRIED)
    while True:
        curDateTime = pd.to_datetime(datetime.now())
        minute = curDateTime.minute
        if minute % TIME_PRIED == 0:
            time_pried = int((curDateTime - last_curDateTime).total_seconds() / 60)
            backtrack_data = main(curDateTime, backtrack_data, farm_id, label_number, times, time_pried, minute)
            times += 1
            last_curDateTime = curDateTime


def func(listTemp, n):
    for i in range(0, len(listTemp), n):
        yield listTemp[i:i + n]


def main(curDateTime, backtrack_data, farm_id, label_number, times, time_pried, minute, report=True, clean=True,
         check=True):
    print('========start=========')
    print('运行时间:', datetime.now())
    print('回溯时间:', curDateTime)
    if farm_id is None:
        obj = DataSet()
        farm_id = obj.get_usable_farm_id()
        obj.close()
    st = time.time()
    data_obj = DataSet()
    backtrack_markIdList = data_obj.get_all_markId_from_animal_mask(label_number=label_number, farm_id=farm_id)
    if times == 0:
        tempList = func(backtrack_markIdList, EAR_TAG_BATCH)
        print('总数据量: %s' % len(backtrack_markIdList))
        last_data_size = 0
        for index, markIdList in enumerate(tempList):
            t1 = time.time()
            backtrack_data = data_obj.dataSetGet(curDateTime=curDateTime, backtrack_markIdList=markIdList,
                                                 backtrack_data=backtrack_data, times=times, time_pried=time_pried)

            ear_tag_obj = EarTagStateModel()
            ear_tag_obj.backtrack_for_detect(curDateTime=curDateTime, backtrack_data=backtrack_data,
                                             backtrack_markIdList=markIdList)
            ear_tag_obj.close()
            if minute % DEATH_PRIED == 0:
                death_model = DeathModel()
                death_model.backtrack_for_detect(curDateTime=curDateTime, backtrack_data=backtrack_data,
                                                 backtrack_markIdList=markIdList)
                death_model.close()
            t2 = time.time()
            print('第%d批次运行成功' % (index + 1), '\t运行时间:%s' % (t2-t1))
            if len(backtrack_data) == 0:
                print('读取的总数据量:', 0)
            else:
                data_size = backtrack_data['markId'].nunique()
                print('读取的总数据量:', data_size, '\t本批次读取的数据量:', data_size-last_data_size)
                last_data_size = data_size
    else:
        backtrack_data = data_obj.dataSetGet(curDateTime=curDateTime, backtrack_markIdList=backtrack_markIdList,
                                             backtrack_data=backtrack_data, times=times, time_pried=time_pried)
        ear_tag_obj = EarTagStateModel()
        ear_tag_obj.backtrack_for_detect(curDateTime=curDateTime, backtrack_data=backtrack_data,
                                         backtrack_markIdList=backtrack_markIdList)
        ear_tag_obj.close()
        if minute % DEATH_PRIED == 0:
            death_model = DeathModel()
            death_model.backtrack_for_detect(curDateTime=curDateTime, backtrack_data=backtrack_data,
                                             backtrack_markIdList=backtrack_markIdList)
            death_model.close()
    data_obj.close()
    et = time.time()
    print('运行时间:', et - st, '秒')
    print('=========end==========')
    print(' ')
    if report:
        if curDateTime.hour == 0 and curDateTime.minute == 0:
            try:
                report_obj = Report(send=True)
                report_obj.day_report(curDateTime)
                if curDateTime.day_of_week == 0:
                    report_obj.weak_report(curDateTime)
                if curDateTime.day == 1:
                    report_obj.month_report(curDateTime)
            except Exception as e:
                send_error_report('Report Error ', e)
            else:
                report_obj.close()
                print('Send Report Successful')
    if clean:
        if curDateTime.hour == 0 and curDateTime.minute == 0:
            try:
                clean_obj = DataClean()
                clean_obj.clean_data(curDateTime, delete_interval=30)
            except Exception as e:
                send_error_report('Clean Data Error ', e)
            else:
                clean_obj.close()
                print('Clean Data Successful')

    if check:
        if curDateTime.hour == 0 and curDateTime.minute == 0:
            try:
                check_obj = DataCheck()
                check_obj.data_check()

            except Exception as e:
                send_error_report('Check Data Error ', e)
            else:
                check_obj.close()
                print('Check Data Successful')

    return backtrack_data


if __name__ == '__main__':
    _farm_id = FIRM_ID
    _label_number = LABEL_NUMBER
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--time", type=str, default=None)
    args = parser.parse_args()
    _backtrack_time = args.time

    if _backtrack_time is None:
        timing(_farm_id, _label_number)
    else:
        _backtrack_time = pd.to_datetime(_backtrack_time)
        _backtrack_data, _times, _curDateTime = recall(_backtrack_time, _farm_id, _label_number)
        timing(_farm_id, _label_number, _backtrack_data, _times, _curDateTime)
