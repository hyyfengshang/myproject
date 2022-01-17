import pandas as pd
from DataSet import DataSet
from datetime import timedelta, datetime
from param import *
from EarTagStateModel import EarTagStateModel
from DeathModel import DeathModel
import os


def main(datafile, curDateTime, label_number):
    print(curDateTime)
    end_dt = curDateTime
    start_dt = end_dt - timedelta(hours=BACK_PERIOD)

    data_obj = DataSet()
    backtrack_markIdList = data_obj.get_all_markId_from_animal_mask(label_number)
    markId = backtrack_markIdList[0]
    backtrack_data = data_obj.dataSetGetLocal(datafile, markId, start_dt, end_dt)
    data_obj.close()

    ear_obj = EarTagStateModel()
    ear_obj.backtrack_for_detect(curDateTime, backtrack_data, backtrack_markIdList)
    ear_obj.close()

    death_obj = DeathModel(to_mysql=True, ignore_state=False)
    death_obj.backtrack_for_detect(curDateTime, backtrack_data, backtrack_markIdList)
    death_obj.close()


def val(datafile, label_number):
    data = pd.read_csv(datafile)
    start_dt = pd.to_datetime(data.iloc[0]['createTime'])
    end_dt = pd.to_datetime(data.iloc[-1]['createTime'])
    while start_dt < end_dt:
        main(datafile, start_dt, label_number)
        start_dt = start_dt + timedelta(minutes=TIME_PRIED)


def batch_val(data_path):
    datafiles = os.listdir(data_path)
    for data_name in datafiles:
        datafile_ = data_path + '/' + data_name
        label_number_ = data_name.split('.')[0]
        print(label_number_)
        val(datafile_, label_number_)


if __name__ == '__main__':
    # datafile = '../data/original_data/ear_tag_off_data/100000100013052.csv'
    # val(datafile, 100000100013052 )

    data_path = '../data/original_data/ear_tag_off_data'
    batch_val(data_path)
