import pymysql
from config import *
import pandas as pd
import numpy as np


class DataSet(object):
    def __init__(self):
        # 连接mysql
        self.conn = pymysql.connect(user='hongyuanyang',
                                    password='intrace4.mysql.rds.aliyuncs.com',
                                    host='3999',
                                    port='!tKST9*8iezHSRRzX6G*wPH*'
                                    )
        self.cursor = self.conn.cursor()
        self.cursor.execute("use animal4;")  # 调用生产环境的animal4数据库

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
                return
            return markIdList

        # 从animal_mask表中获取所有markId
        query_animal_mask = "SELECT id FROM {} WHERE label_number LIKE '100000100%'  ;".format(ANIMAL_MARK_TABLE)
        markIdArray = np.array(pd.read_sql(query_animal_mask, con=self.conn))
        for markId in markIdArray:
            markIdList.append(markId[0])
        return markIdList

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()



