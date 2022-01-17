from main import val
import pandas as pd
from datetime import timedelta, datetime
from DataSet import DataSet
from param import *


def func(listTemp, n):
    for i in range(0, len(listTemp), n):
        yield listTemp[i:i + n]


def main():
    obj = DataSet()
    all_labelList = obj.get_all_label_number_from_animal_mask()
    obj.close()
    tempList = func(all_labelList, 100)
    for labelList in tempList:
        val(start_time=START_TIME, end_time=END_TIME, farm_id=FIRM_ID, label_number=labelList, report=False)


if __name__ == '__main__':
    main()