"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 耳标状态和死亡判别模型
"""

from datetime import datetime, timedelta
import pymongo
import calendar
import pandas as pd
import numpy as np
from bson import Int64
from config import *
from param import *
import os
from log import logger
import pymysql


class DataSet(object):
    """
        获取猪的活动数据，并返回数据集
    """

    def __init__(self):
        # 连接mysql
        self.conn = pymysql.connect(user=MYSQL_USER,
                                    password=MYSQL_PASSWORD,
                                    host=MYSQL_HOST,
                                    port=MYSQL_PORT
                                    )
        self.cursor = self.conn.cursor()
        self.cursor.execute("use animal4;")  # 调用生产环境的animal4数据库
        logger.info("Connect to MySQL successfully!")

    def markId_to_label_number(self, markId):
        query_label = 'SELECT label_number FROM animal_mark WHERE id = %s' % markId
        df = pd.read_sql(query_label, con=self.conn)
        if len(df) == 0:
            return
        label = np.array(df)[0][0]
        return label

    def label_number_to_markId(self, label_number):
        query_markId = 'SELECT id FROM animal_mark WHERE label_number =%s' % label_number
        df = pd.read_sql(query_markId, con=self.conn)
        if len(df) == 0:
            return
        markId = np.array(df)[0][0]
        return markId

    def get_usable_farm_id(self):
        """获得启用的农场id"""
        query_farm_id = "SELECT id  FROM sys_farm WHERE `delete_flag` = 0 AND destination_type_id= 0 AND STATUS = TRUE"
        farm_id_data = np.array(pd.read_sql(query_farm_id, con=self.conn))
        farm_id_list = [str(i[0]) for i in farm_id_data]
        return farm_id_list

    def get_all_label_number_from_animal_mask(self):
        """获取所有录入的耳标号"""
        labelList = []
        query_animal_mask = "SELECT label_number FROM {} WHERE label_number LIKE '100000100%'  ;".format(ANIMAL_MARK_TABLE)
        labelArray = np.array(pd.read_sql(query_animal_mask, con=self.conn))
        for label in labelArray:
            labelList.append(label[0])
        return labelList

    def get_all_markId_from_animal_mask(self, label_number=None, farm_id=None):
        """"
        获取需要提取数据的猪的id
        :param:label_number 耳标号
        :param:farm_id  农场编号
        :return: markIdList  所有动物唯一编号的列表
        """
        markIdList = []
        # 从animal_mask表中获取某个耳标的markId
        if label_number is not None:
            if isinstance(label_number, list):
                for _label_number in label_number:
                    query_one_markId = "SELECT id FROM {} WHERE label_number = {};".format(ANIMAL_MARK_TABLE,
                                                                                           _label_number)
                    df = pd.read_sql(query_one_markId, con=self.conn)
                    if len(df) == 0:
                        return markIdList
                    markId = np.array(df)[0][0]
                    markIdList.append(markId)
            elif isinstance(label_number, int):
                query_one_markId = "SELECT id FROM {} WHERE label_number = {};".format(ANIMAL_MARK_TABLE, label_number)
                df = pd.read_sql(query_one_markId, con=self.conn)
                if len(df) == 0:
                    return markIdList
                markId = np.array(df)[0][0]
                markIdList.append(markId)
            elif isinstance(label_number, str):
                query_one_markId = "SELECT id FROM {} WHERE label_number = {};".format(ANIMAL_MARK_TABLE, label_number)
                df = pd.read_sql(query_one_markId, con=self.conn)
                if len(df) == 0:
                    return markIdList
                markId = np.array(df)[0][0]
                markIdList.append(markId)
            else:
                logger.error('Incorrect label number type!')
                return
            return markIdList

        # 从animal_mask表中获取某个猪场所有的markId
        if farm_id is not None:
            if isinstance(farm_id, list):
                for _farm_id in farm_id:
                    query_farm_markId = "SELECT id FROM {} WHERE label_number LIKE '100000100%' and farm_id={};".format(
                        ANIMAL_MARK_TABLE, _farm_id)
                    markIdArray = np.array(pd.read_sql(query_farm_markId, con=self.conn))
                    for markId in markIdArray:
                        markIdList.append(markId[0])
            elif isinstance(farm_id, int):
                query_farm_markId = "SELECT id FROM {} WHERE label_number LIKE '100000100%' and farm_id={};".format(
                    ANIMAL_MARK_TABLE, farm_id)
                markIdArray = np.array(pd.read_sql(query_farm_markId, con=self.conn))
                for markId in markIdArray:
                    markIdList.append(markId[0])
            elif isinstance(farm_id, str):
                query_farm_markId = "SELECT id FROM {} WHERE label_number LIKE '100000100%' and farm_id={};".format(
                    ANIMAL_MARK_TABLE, farm_id)
                markIdArray = np.array(pd.read_sql(query_farm_markId, con=self.conn))
                for markId in markIdArray:
                    markIdList.append(markId[0])
            else:
                logger.error('Incorrect farm id type')
            return markIdList

        # 从animal_mask表中获取所有markId
        query_animal_mask = "SELECT id FROM {} WHERE label_number LIKE '100000100%'  ;".format(ANIMAL_MARK_TABLE)
        markIdArray = np.array(pd.read_sql(query_animal_mask, con=self.conn))
        for markId in markIdArray:
            markIdList.append(markId[0])
        return markIdList

    def dataSet(self, start_dt, end_dt, markIdList=None):
        """
        获取猪的数据集
        :param start_dt: 开始时间
        :param end_dt: 结束时间
        :param markIdList: 猪的id组成的列表
        :return:
        """
        assert isinstance(start_dt, datetime)
        assert isinstance(end_dt, datetime)
        assert start_dt < end_dt
        if markIdList is None:
            return

        client = pymongo.MongoClient(host=MONGO_HOST,
                                     port=MONGO_PORT)
        db = client.get_database(MONGO_DB)
        env_temp_record_col = db.get_collection(MONGO_COLLECTIONS_ENV_TEMPERATURE_RECORD)

        # 查询env_temperature_record集合中文档数据
        query1 = {"$and":[
            {
                u"time":{
                    u"$gte":start_dt.strftime("%Y%m%d%H%M%S")
                }
            },
            {
                u"time":{
                    u"$lte":end_dt.strftime("%Y%m%d%H%M%S")
                }
            }
        ]}

        projection = {"deviceCode":u"$deviceCode", "temperature":u"$temperature", "time":u"$time", "_id":0}
        sort = [(u"time", 1)]

        # 查询回溯时间段内的环境温度表里的数据
        cursor_ete = env_temp_record_col.find(query1, projection=projection, no_cursor_timeout=True)

        try:
            df_etr = pd.DataFrame(list(cursor_ete))
            if len(df_etr) <= 0:
                logger.error("df_etr returns an empty DataFrame !")
                return

        except Exception as e:
            logger.error("Reading environment temperature data from MongoDB failed: {}".format(e))
        else:
            if not {"time"}.issubset(df_etr.columns):
                logger.error("ValueError: There is no 'time' column in the returned df_etr! ")

            df_etr['createTime'] = df_etr['time'].apply(lambda x:datetime.strptime(x[:-2], "%Y%m%d%H%M"))
            df_etr = df_etr.drop(['time'], axis=1)

            if not {'temperature', 'deviceCode'}.issubset(df_etr.columns):
                logger.error("ValueError: There is no 'temperature, deviceCode' columns in the returned df_etr! ")

            # 环境温度异常值判断
            df_etr['temperature'] = df_etr['temperature'].apply(np.float32)
            env_temp = df_etr['temperature'].values
            df_etr['temperature'] = np.where((env_temp > -30) & (env_temp < 45), env_temp,
                                             np.NaN)  # 设置正常环境温度在-30℃到45℃之间

            df_etr_gp = df_etr.groupby(by=['createTime', 'deviceCode'], as_index=False). \
                agg(temperature=('temperature', 'mean'))

            logger.info("Reading environment temperature data from MongoDB successfully")

        # 查询temp_history集合中文档数据
        assert start_dt.year == start_dt.year
        # 对时间边界做处理

        col = MONGO_COLLECTIONS_TEMP_HISTORY
        periods = []
        # 对时间进行划分 ==> 适配temp_history 按月分库
        cur_year = start_dt.year
        while start_dt.month < end_dt.month:
            next_month = calendar._nextmonth(cur_year, start_dt.month)
            next_date = datetime(*next_month, day=1)
            collection = col + "_" + "{}".format(start_dt.strftime('%Y%m'))
            periods.append((start_dt, next_date, collection))

            start_dt = next_date
        else:
            if start_dt.month == datetime.now().month:
                periods.append((start_dt, end_dt, col))
            else:
                collection = col + "_" + "{}".format(start_dt.strftime('%Y%m'))
                periods.append((start_dt, end_dt, collection))
        # print(periods)
        df_th = pd.DataFrame()
        for start_dt, end_dt, col in periods:
            query2 = {"$and":[
                {
                    u"createTime":{
                        u"$gte":start_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                },
                {
                    u"createTime":{
                        u"$lte":end_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                },
                {
                    u"markId":{
                        u"$in":[
                            Int64(markId) for markId in markIdList
                        ]
                    }
                }
            ]}

            projection = {"markId":u"$markId", "deviceCode":u"$deviceCode", "createTime":u"$createTime",
                          "temperature":u"$temperature", "xAxis":u"$xAxis", "yAxis":u"$yAxis", "zAxis":u"$zAxis",
                          "_id":0}

            temp_history_col = db.get_collection(col)

            cursor_th = temp_history_col.find(query2, projection=projection, no_cursor_timeout=True)

            _df = pd.DataFrame(list(cursor_th))
            df_th = pd.concat([df_th, _df], axis=0)
        try:
            if len(df_th) <= 0:
                logger.error("df_th returns an empty DataFrame !")
                return
        except Exception as e:
            logger.error("Reading environment temperature data from MongoDB failed: {}".format(e))
        else:
            if not {'createTime'}.issubset(df_th.columns):
                logger.error("There is no 'createTime' column in the returned df_th !")

            df_th['createTime'] = df_th['createTime'].apply(lambda x:datetime.strptime(x[:-3], "%Y-%m-%d %H:%M"))
            if not {'xAxis', 'yAxis', 'zAxis'}.issubset(df_th.columns):
                df_th['xyz_axis_abs_sum'] = [np.NaN] * len(df_th)
            else:
                df_th['xyz_axis_abs_sum'] = np.abs(df_th['xAxis']) + np.abs(df_th['yAxis']) + np.abs(df_th['zAxis'])

            if not {'temperature', 'xyz_axis_abs_sum'}.issubset(df_th.columns):
                raise ValueError("There is no 'temperature, xyz_axis_abs_sum' columns in the returned df_th! ")

            df_th['temperature'] = df_th['temperature'].apply(np.float32)
            # th_temp = df_th['temperature'].values
            # df_th['temperature'] = np.where((th_temp > 10) & (th_temp < 42), th_temp, np.NaN)  # 设置正常体温在10℃到42℃之间

            df_th_gp = df_th.groupby(by=['markId', 'createTime', 'deviceCode'], as_index=False).agg(
                temperature=('temperature', 'mean'),
                xyz_axis_abs_sum=('xyz_axis_abs_sum', 'mean'),
                xAxis=('xAxis', self.abs_max),
                yAxis=('yAxis', self.abs_max),
                zAxis=('zAxis', self.abs_max))

            logger.info("Reading earTag temperature and sport data from MongoDB successfully")

        finally:
            client.close()  # 关闭mongodb 连接
        # 以createTime, deviceCode为索引，合并df_etr, df_th 两个DataFrame
        df = pd.merge(df_th_gp, df_etr_gp, how='left', on=['createTime', 'deviceCode'], suffixes=['_earTag', '_env'])
        df_gp = df.groupby(by=['markId', 'createTime'], as_index=False).agg(
            temperature_earTag=('temperature_earTag', 'mean'),
            xyz_axis_abs_sum=('xyz_axis_abs_sum', 'mean'),
            temperature_env=('temperature_env', 'mean'),
            xAxis=('xAxis', self.abs_max),
            yAxis=('yAxis', self.abs_max),
            zAxis=('zAxis', self.abs_max))

        cols = ["markId", "createTime", "temperature_earTag", "temperature_env", 'xAxis', 'yAxis', 'zAxis',
                "xyz_axis_abs_sum"]
        df_gp = df_gp[cols]
        return df_gp

    @staticmethod
    def abs_max(series):
        """
            Returns the maximum value in absolute value
        :return:
        """
        assert isinstance(series, pd.Series)
        index = np.argmax(series.abs())
        return series.iat[index]

    def dataSetGet(self, curDateTime, backtrack_markIdList, backtrack_data=None, times=0, time_pried=0):
        """
        分布式数据流获取
        :param backtrack_markIdList: 回溯的markId
        :param curDateTime: 当前时间
        :param backtrack_data: 上一批次数据
        :param times: 提取数据的批次
        :param time_pried:与上一次数据提取的时间间隔
        :return: df
        """
        end_dt = datetime(year=curDateTime.year, month=curDateTime.month, day=curDateTime.day,
                          hour=curDateTime.hour, minute=curDateTime.minute, second=0)  # 定时运行
        if len(backtrack_markIdList) == 0:
            logger.error("Backtrack markIdList is null")
            return backtrack_data
        if times == 0:
            start_dt = end_dt - timedelta(hours=BACK_PERIOD)  # 回溯24个小时的时间
            backtrack_data_pried = self.dataSet(start_dt, end_dt, backtrack_markIdList)  # 获取24个小时的数据
            backtrack_data = pd.concat([backtrack_data, backtrack_data_pried])
            return backtrack_data
        else:
            start_dt = end_dt - timedelta(minutes=time_pried)  # 回溯运行的间隔时间
            backtrack_pried_data = self.dataSet(start_dt, end_dt, backtrack_markIdList)  # 获取新的一段数据
            if backtrack_data is None:
                return backtrack_pried_data
            backtrack_data = backtrack_data[(backtrack_data['createTime'] < end_dt) & (
                    backtrack_data['createTime'] >= end_dt - timedelta(hours=BACK_PERIOD))]  # 剔除旧的一段数据
            backtrack_data = pd.concat([backtrack_data, backtrack_pried_data])  # 合并为新的数据区间
            return backtrack_data

    def get_Day_Ago_Data(self, markId, curDateTime):
        """
        获取一天前的数据
        :param markId: 猪的id
        :param curDateTime: 当前时间
        :return: 环境温度
        """
        end_dt = datetime(year=curDateTime.year, month=curDateTime.month, day=curDateTime.day,
                          hour=curDateTime.hour, minute=curDateTime.minute, second=0) - timedelta(days=1)
        start_dt = end_dt - timedelta(hours=TAG_OFF_PERIOD)
        assert isinstance(start_dt, datetime)
        assert isinstance(end_dt, datetime)
        assert start_dt < end_dt
        markIdList = [markId]
        data = self.dataSet(start_dt, end_dt, markIdList)
        return data

    @staticmethod
    def dataSetGetLocal(datafile, markId, start_dt, end_dt):
        datas = pd.read_csv(datafile)
        data = pd.DataFrame(datas)
        data['xyz_axis_abs_sum'] = np.abs(data['xAxis']) + np.abs(data['yAxis']) + np.abs(data['zAxis'])
        data['markId'] = markId
        data['createTime'] = pd.to_datetime(data['createTime'])
        data = data[(data['createTime'] >= pd.to_datetime(start_dt)) & (data['createTime'] <= pd.to_datetime(end_dt))]
        return data

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    # obj = PigDataSet()
    # markId_ = [607894753266106368]
    # curDateTime_ = datetime.now()
    # t = obj.Get_Default_Environment_Temperature(markId_, curDateTime_)
    # print(t)
    obj = DataSet()
    farm_id = obj.get_usable_farm_id()
    # markId_list = obj.get_all_markId_from_animal_mask(farm_id=farm_id)
    # print(len(markId_list))
    # all_markId_list = obj.get_all_markId_from_animal_mask()
    # print(len(all_markId_list))
    # obj.close()
    print(farm_id)
    if str(660171657482080256) in farm_id:
        print('t')
