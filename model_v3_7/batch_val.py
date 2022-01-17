from main import val
import pandas as pd
from datetime import timedelta, datetime
from param import TIME_PRIED


def bach_val(filexpath, lead_time, back_time):
    val_datas = open(filexpath, encoding='utf-8')
    for data in val_datas:
        if data.find('\t') == -1:
            label_number = data.split(' ')[0]
            curDateTime = pd.to_datetime(data.split(' ')[1] + ' ' + data.split(' ')[2])
        else:
            label_number = data.split('\t')[0]
            curDateTime = pd.to_datetime(data.split('\t')[1])
        curDateTime = datetime(year=curDateTime.year, month=curDateTime.month, day=curDateTime.day,
                               hour=curDateTime.hour, minute=curDateTime.minute - curDateTime.minute % TIME_PRIED,
                               second=0)
        start_time = curDateTime - timedelta(hours=lead_time)
        end_time = start_time + timedelta(hours=back_time)
        print(label_number)
        val(start_time, end_time, label_number=label_number)


if __name__ == '__main__':
    # # 验证所有数据
    # val_xpath = '../data/data_08'
    # val_files = os.listdir(val_xpath)
    # for val_file in val_files:
    #     val_txt = val_xpath + '/' + val_file
    #     _lead_time = 0.5  # 回溯前的时间
    #     _back_time = 12   # 回溯后的时间
    #     bach_val(val_txt, _lead_time, _back_time)

    # 验证单类数据
    val_txt = '../data/death_cow_data.txt'
    _lead_time = 6  # 回溯前的时间
    _back_time = 24  # 回溯后的时间
    bach_val(val_txt, _lead_time, _back_time)
