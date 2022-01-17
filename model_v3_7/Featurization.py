import numpy as np
import pandas as pd
from datetime import timedelta, datetime
from param import *


class EarTagModelFeature(object):
    """耳标模型特征工程"""

    def __init__(self, new_df=None, old_df=None, back_pried_df=None):
        self.new_df = new_df
        self.old_df = old_df
        self.back_pried_df = back_pried_df

    def mean_env_temp(self):
        mean_env_temp = np.mean(self.new_df['temperature_env'])  # 环境温度均值
        return mean_env_temp

    def mean_ear_temp(self):
        mean_ear_temp = np.mean(self.new_df['temperature_earTag'])  # 耳标温度均值
        return mean_ear_temp

    def mean_temp_diff(self):
        mean_temp_diff = self.mean_ear_temp() - self.mean_env_temp()  # 温度差均值
        return mean_temp_diff

    def mean_animal_temp(self):
        mean_animal_temp = np.mean(self.back_pried_df['temperature_earTag'])  # 正常体温均值
        mean_animal_temp = np.where(mean_animal_temp > MINIMUM_EAR_TEMP, mean_animal_temp, MINIMUM_EAR_TEMP)
        return mean_animal_temp

    def temp_threshold(self):
        temp_threshold = self.mean_animal_temp() - self.mean_env_temp() - TAG_OFF_TEMP_THRESHOLD  # 耳标脱落阈值
        temp_threshold = np.where(temp_threshold > MINIMUM_TEMP_CHANGE_THRESHOLD, temp_threshold,
                                  MINIMUM_TEMP_CHANGE_THRESHOLD)  # 设置最低温度差阈值
        temp_threshold = np.where(temp_threshold < MAXIMUM_TEMP_CHANGE_THRESHOLD, temp_threshold,
                                  MAXIMUM_TEMP_CHANGE_THRESHOLD)  # 设置最高温度差阈值
        return temp_threshold

    def temp_change(self, hour=0, type_conf_id=12):
        # temp_change = np.median(old_df['temperature_earTag']) - np.median(new_df['temperature_earTag'])  # 温度变化量  v1
        self.old_df.reset_index(inplace=True, drop=True)
        self.new_df.reset_index(inplace=True, drop=True)
        # temp_change = old_df.iloc[0][ 'temperature_earTag'] - old_df.iloc[-1][ 'temperature_earTag']  # 温度变化量  v2修改版 脱落后数据缺失容易漏判
        temp_change = self.old_df.iloc[0]['temperature_earTag'] - np.median(
            self.new_df['temperature_earTag'])  # 温度变化量  v3修改版
        if 7 < hour < 18 and type_conf_id in TYPE_OFF_COW_LIST:  # 对于牛处于中午的情况额外处理
            temp_change = self.old_df.iloc[0]['temperature_earTag'] - np.min(
                self.new_df['temperature_earTag'])  # 温度变化量  v3修改版
        return temp_change

    def temp_deviation(self):
        temp_deviation = self.temp_change() - self.temp_threshold()
        return temp_deviation

    def sport_rate(self, sport_threshold):
        self.new_df['xyz_axis_abs_sum'] = np.abs(self.new_df['xAxis']) + np.abs(self.new_df['yAxis']) + np.abs(
            self.new_df['zAxis'])
        sport_rate = np.sum(self.new_df['xyz_axis_abs_sum'].values < sport_threshold) / len(self.new_df)  # 运动占比
        return sport_rate

    def diff_rate(self, delta_threshold):
        diff_rate = np.sum(
            (self.new_df['temperature_earTag'] - self.new_df['temperature_env']).values < delta_threshold) / len(
            self.new_df)
        return diff_rate

    def maximum_rate_of_temp_change(self):
        """
        提取最大相邻温度变化量
        :return:
        """
        max_temp_change = 0
        df = self.old_df
        for index, row in enumerate(df.iterrows()):
            if index == 0:
                continue
            end_temp = row[1]['temperature_earTag']
            start_temp = df.iloc[index - 1]['temperature_earTag']
            temp_change = start_temp - end_temp
            if temp_change > max_temp_change:
                max_temp_change = temp_change
        return max_temp_change

    def maximum_rate_of_temp_change_rate(self):
        """
        提取最大温度变化率
        :return:
        """
        max_temp_change_rate = 0
        df = self.old_df
        for index, row in enumerate(df.iterrows()):
            if index == 0:
                continue
            end_dt = pd.to_datetime(row[1]['createTime'])
            start_dt = pd.to_datetime(df.iloc[index - 1]['createTime'])
            end_temp = row[1]['temperature_earTag']
            start_temp = df.iloc[index - 1]['temperature_earTag']
            temp_change = start_temp - end_temp
            time_delete = (end_dt - start_dt).seconds / 60
            temp_change_rate = temp_change / time_delete
            if temp_change_rate > max_temp_change_rate:
                max_temp_change_rate = temp_change_rate
        return max_temp_change_rate

    def maximum_rate_of_temp_change_with_time(self, time_pried=10):
        """
        提取给定区间最大温度变化量
        :param time_pried: 数据区间/min
        :return:
        """
        max_temp_change_rate = 0
        df = self.old_df
        for index, row in enumerate(df.iterrows()):
            start_dt = pd.to_datetime(df.iloc[index]['createTime'])
            end_dt = pd.to_datetime(start_dt + timedelta(minutes=time_pried))
            start_temp = row[1]['temperature_earTag']
            end_dt = min(pd.to_datetime(df['createTime']), key=lambda x:abs(x - end_dt))
            end_temp = df.loc[(pd.to_datetime(df['createTime']) == end_dt), 'temperature_earTag'].values[0]
            temp_change_rate = start_temp - end_temp
            if temp_change_rate > max_temp_change_rate:
                max_temp_change_rate = temp_change_rate
        return max_temp_change_rate

    def extremum_temp_change_with_time(self, time_pried=20, interval=1):
        """提取给定时间区间温度变化极值
        :param interval: 切片间隔
        :param time_pried:时间区间 /min

        """
        max_extremum_temp_change = 0
        df = self.old_df
        for index, row in enumerate(df.iterrows()):
            if index % interval != 0:
                continue
            start_dt = pd.to_datetime(df.iloc[index]['createTime'])
            end_dt = pd.to_datetime(start_dt + timedelta(minutes=time_pried))
            end_dt = min(pd.to_datetime(df['createTime']), key=lambda x:abs(x - end_dt))
            if end_dt == start_dt:
                if index + 1 >= len(df):
                    continue
                end_dt = pd.to_datetime(df.iloc[index + 1]['createTime'])
            extremum_df = df.loc[
                (pd.to_datetime(df['createTime']) > start_dt) & (pd.to_datetime(df['createTime']) <= end_dt)]
            extremum_temp_change = np.max(extremum_df['temperature_earTag']) - np.min(extremum_df['temperature_earTag'])
            if extremum_temp_change > max_extremum_temp_change:
                max_extremum_temp_change = extremum_temp_change
        return max_extremum_temp_change

    def extremum_temp_change_with_pried(self, pried, interval):
        max_extremum_temp_change = 0
        df = self.old_df
        for index, row in enumerate(df.iterrows()):
            if index % interval != 0:
                continue
            extremum_df = df.iloc[index:index + pried]
            extremum_temp_change = np.max(extremum_df['temperature_earTag']) - np.min(extremum_df['temperature_earTag'])
            if extremum_temp_change > max_extremum_temp_change:
                max_extremum_temp_change = extremum_temp_change
        return max_extremum_temp_change


class DeathModelFeature(object):
    """死亡模型特征工程"""

    def __init__(self, new_df):
        self.new_df = new_df

    def temp_diff_person_coefficient(self, con=-0.05, cof=5.5):
        self.new_df['temp_diff'] = self.new_df['temperature_earTag'] - self.new_df['temperature_env']
        y = self.new_df['temp_diff'].values
        x = np.linspace(0, 20, len(y))
        z = cof * np.exp(con * x)  # 温度差死亡函数
        p = np.corrcoef(y, z, rowvar=False)[0][1]
        return p

    def temp_ear_person_coefficient(self, con=-0.025, cof=35.5):
        y = self.new_df['temperature_earTag'].values
        x = np.linspace(0, 20, len(y))
        z = cof * np.exp(con * x)  # 体温死亡函数
        p = np.corrcoef(y, z, rowvar=False)[0][1]
        return p

    def sport_rate(self, sport_threshold):
        self.new_df['xyz_axis_abs_sum'] = np.abs(self.new_df['xAxis']) + np.abs(self.new_df['yAxis']) + np.abs(
            self.new_df['zAxis'])
        sport_rate = np.sum(self.new_df['xyz_axis_abs_sum'].values < sport_threshold) / len(self.new_df)  # 运动占比
        return sport_rate

    def mean_env_temp(self):
        mean_env_temp = np.mean(self.new_df['temperature_env'])  # 环境温度均值
        return mean_env_temp

    def mean_ear_temp(self):
        mean_ear_temp = np.mean(self.new_df['temperature_earTag'])  # 耳标温度均值
        return mean_ear_temp

    def mean_temp_diff(self):
        mean_temp_diff = self.mean_ear_temp() - self.mean_env_temp()  # 温度差均值
        return mean_temp_diff

    def temp_diff_rate(self, temp_diff_threshold):
        temp_diff_rate = np.sum(
            (self.new_df['temperature_earTag'] - self.new_df['temperature_env']).values < temp_diff_threshold) / len(
            self.new_df)
        return temp_diff_rate


class TsfreshFreture(object):
    def __init__(self, new_df):
        self.new_df = new_df


if __name__ == '__main__':
    # data_file = 'data\\new_original_data\\original_death_cow_data\\100000100007820.csv'
    # df = pd.read_csv(data_file)
    # item = EarTagModelFeature(old_df=df)
    # result = item.extremum_temp_change_with_pried(pried=20, interval=5)
    # print(result)
    data_xpath = '../data/original_data/ear_tag_off_data'
    import os
    datafiles = os.listdir(data_xpath)
    for datafile in datafiles:
        df = pd.read_csv(data_xpath + '/' + datafile)
        item = EarTagModelFeature(old_df=df)
        result = item.extremum_temp_change_with_pried(pried=20, interval=5)
        print(datafile, '\t', result)

