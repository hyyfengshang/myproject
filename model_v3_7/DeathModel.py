# -*- coding:utf-8 -*-

"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 死亡判别模型
"""

import traceback
from tqdm import tqdm
from datetime import datetime, timedelta
import pymysql
from config import *
from param import *
from Featurization import DeathModelFeature
import pandas as pd
import numpy as np
from log import logger
from report import send_error_report
import warnings
import joblib
import tensorflow as tf

np.seterr(divide='ignore', invalid='ignore')
warnings.filterwarnings("ignore")


class DeathModel(object):
    def __init__(self, to_mysql=True, ignore_state=False):
        # 1. 连接mysql
        self.conn = pymysql.connect(user=MYSQL_USER,
                                    password=MYSQL_PASSWORD,
                                    host=MYSQL_HOST,
                                    port=MYSQL_PORT
                                    )
        self.cursor = self.conn.cursor()
        self.cursor.execute("use animal4;")  # 调用生产环境的animal4数据库
        self.to_mysql = to_mysql
        self.ignore_state = ignore_state
        logger.info("Connect to MySQL successfully!")

    def backtrack_for_detect(self, curDateTime, backtrack_data, backtrack_markIdList):
        """
        :param to_mysql: 是否写入数据库
        :param ignore_state: 忽略数据状态
        :param backtrack_markIdList:回溯的markId
        :param backtrack_data: 回溯的数据
        :param curDateTime: 读取时间
        :return: 模型判别结果
        """
        self.curDateTime = curDateTime
        self.end_dt = datetime(year=curDateTime.year, month=curDateTime.month, day=curDateTime.day,
                               hour=curDateTime.hour, minute=curDateTime.minute, second=0)  # 定时运行
        if len(backtrack_markIdList) == 0:
            logger.error("Backtrack markIdList is null")
            return

        if backtrack_data is None or len(backtrack_data) == 0:
            logger.error("Backtrack data is null")
            return

        self.start_dt = self.end_dt - timedelta(hours=DEATH_PERIOD * 2)  # 回溯的总区间

        print('猪的死亡判别模型')

        # 获得所有确定的死猪的markId
        query_have_death_markId = "SELECT mark_id FROM {} WHERE pre_audit_status = 1".format(DEATH_MODEL_TABLE)
        have_death_markId = np.array(pd.read_sql(query_have_death_markId, con=self.conn))
        self.have_death_markId_list = [str(i[0]) for i in have_death_markId]

        # 获得所有未确定的死猪的markId
        query_unconfirmed_markId = "SELECT mark_id FROM {} WHERE pre_audit_status = 0 and death_type = 1".format(
            DEATH_MODEL_TABLE)
        unfirmed_markId = np.array(pd.read_sql(query_unconfirmed_markId, con=self.conn))
        self.unfirmed_markId_list = [str(i[0]) for i in unfirmed_markId]

        # 获得需要进行死亡修正判别的猪的markId
        need_judge_death_pig_data_night = "SELECT mark_id FROM {} WHERE death_type = 2".format(DEATH_MODEL_TABLE)
        need_judge_death_pig_night = np.array(pd.read_sql(need_judge_death_pig_data_night, con=self.conn))
        self.need_judge_death_pig_night_list = [str(i[0]) for i in need_judge_death_pig_night]

        need_judge_death_pig_data_day = "SELECT mark_id FROM {} WHERE death_type = 4".format(DEATH_MODEL_TABLE)
        need_judge_death_pig_day = np.array(pd.read_sql(need_judge_death_pig_data_day, con=self.conn))
        self.need_judge_death_pig_day_list = [str(i[0]) for i in need_judge_death_pig_day]

        need_judge_death_pig_data = "SELECT mark_id FROM {} WHERE death_type = 4 or death_type = 2".format(
            DEATH_MODEL_TABLE)
        need_judge_death_pig = np.array(pd.read_sql(need_judge_death_pig_data, con=self.conn))
        self.need_judge_death_pig_list = [str(i[0]) for i in need_judge_death_pig]

        # 获得需要进行死亡修正判别的牛的markId
        need_judge_death_cow_data = "SELECT mark_id FROM {} WHERE death_type = 5".format(DEATH_MODEL_TABLE)
        need_judge_death_cow = np.array(pd.read_sql(need_judge_death_cow_data, con=self.conn))
        self.need_judge_death_cow_list = [str(i[0]) for i in need_judge_death_cow]

        # 获得所有正常数据的markId
        query_normal_data = "SELECT mark_id FROM {} WHERE status = 1 and deal_flag = 0".format(EAR_TAG_MODEL_TABLE)
        normal_data = np.array(pd.read_sql(query_normal_data, con=self.conn))
        self.normal_data_list = [str(i[0]) for i in normal_data]
        self.new_model = tf.keras.models.load_model(DEATH_MODEL_FILE)
        self.scaler = joblib.load(DEATH_SCALER_FILE)

        # 获得脱落待修正的markId
        need_judge_tag_off_data = "SELECT mark_id FROM {} WHERE status = 20".format(EAR_TAG_UNDETERMINED_TABLE)
        need_judge_tag_off = np.array(pd.read_sql(need_judge_tag_off_data, con=self.conn))
        self.need_judge_tag_off_list = [str(i[0]) for i in need_judge_tag_off]

        for markId in tqdm(np.unique(backtrack_markIdList)):
            self.markId = markId
            self.label_number = None
            query_label_number = "SELECT label_number FROM {} WHERE id = {}".format(ANIMAL_MARK_TABLE, self.markId)
            df = pd.read_sql(query_label_number, con=self.conn)
            if len(df) == 0:
                self.__param_insert_to_mysql(output_node='耳标不存在')
                continue
            self.label_number = np.array(df)[0][0]
            query_type_conf_id = "SELECT type_conf_id FROM {} WHERE id = {}".format(ANIMAL_MARK_TABLE, self.markId)
            df = pd.read_sql(query_type_conf_id, con=self.conn)
            if len(df) == 0:
                self.__param_insert_to_mysql(output_node='种类不存在')
                continue
            self.type_conf_id = np.array(df)[0][0]
            backtrack_df = backtrack_data[backtrack_data['markId'] == self.markId].copy()
            try:
                self.__pig_death_judgement(backtrack_df)
            except:
                logger.error('{} Run in pig death model filed!'.format(self.label_number))
                send_error_report(self.label_number, traceback.format_exc())
            # if self.type_conf_id in TYPE_OFF_PIG_LIST:
            #     try:
            #         self.__pig_death_judgement(backtrack_df)
            #     except:
            #         logger.error('{} Run in pig death model filed!'.format(self.label_number))
            #         send_error_report(self.label_number, traceback.format_exc())
            # elif self.type_conf_id in TYPE_OFF_COW_LIST:
            #     try:
            #         self.__cow_death_judgement(backtrack_df)
            #     except:
            #         logger.error('{} Run in cow death model filed!'.format(self.label_number))
            #         send_error_report(self.label_number, traceback.format_exc())
            # else:
            #     self.__param_insert_to_mysql(output_node='没有此类别的死亡判别')

    def __pig_death_judgement(self, backtrack_df):

        # 对于已确定死亡和判断死亡未处理的数据不做处理
        if str(self.markId) in self.have_death_markId_list:
            self.__param_insert_to_mysql(output_node='已确定死亡，不处理')
            return
        if str(self.markId) in self.unfirmed_markId_list:
            self.__param_insert_to_mysql(output_node='已标记死亡未处理')
            return
        if str(self.markId) in self.need_judge_tag_off_list:
            self.__param_insert_to_mysql(output_node='已标记脱落待修正')
            return
        if self.ignore_state is False:
            if str(self.markId) not in self.normal_data_list:
                self.__param_insert_to_mysql(output_node='不属于正常数据')
                return
        # 提取数据
        new_df = backtrack_df.tail(SEQ_LEN)
        new_df.reset_index(inplace=True, drop=True)

        if len(new_df) == 0:
            if str(self.markId) in self.need_judge_death_pig_list and self.curDateTime.hour > DAY_TIME:
                self.death_type = 1
                self.__param_insert_to_mysql(output_node='数据缺失，死亡修正成功')
                if self.to_mysql:
                    self.__death_update_to_mysql()
                return
            self.__param_insert_to_mysql(output_node='数据缺失,不作处理')
            return

        if len(new_df) < SEQ_LEN:
            self.__param_insert_to_mysql(output_node='数据不足，不作处理')
            return
        new_df['xAxis'] = np.abs(new_df['xAxis'])
        new_df['yAxis'] = np.abs(new_df['yAxis'])
        new_df['zAxis'] = np.abs(new_df['zAxis'])
        new_df['temperature_env'] = new_df['temperature_env'].fillna(method='ffill')
        new_df['temperature_env'] = new_df['temperature_env'].fillna(method='bfill')
        if new_df['temperature_env'].isnull().sum() == len(new_df):
            new_df['temperature_env'] = 22
        new_df['temperature_diff'] = new_df['temperature_earTag']-new_df['temperature_env']
        predict_cols = ['temperature_earTag', 'temperature_env', 'temperature_diff', 'xAxis', 'yAxis', 'zAxis']
        predict_df = np.array(new_df[predict_cols])
        x = self.scaler.transform(predict_df.reshape(-1, 1)).reshape(-1, SEQ_LEN, N_CHANNELS)
        x = np.expand_dims(x, axis=1)
        out = np.array(self.new_model(x))
        p1 = out[0][0]
        p2 = out[0][1]
        if str(self.markId) in self.need_judge_death_pig_list:
            query_death_time = "SELECT death_time FROM {} WHERE mark_id = {} ORDER BY death_time DESC LIMIT 1".format(
                DEATH_MODEL_TABLE, self.markId)
            death_time = pd.to_datetime(np.array(pd.read_sql(query_death_time, con=self.conn))[0][0])
            if p1 > DEATH_JUDGE_RATE and DAY_TIME <= self.curDateTime.hour <= NIGHT_TIME:
                self.death_type = 1
                self.__param_insert_to_mysql(output_node='死亡判别成功，跳出修正', p1=p1, p2=p2)
                if self.to_mysql:
                    self.__death_update_to_mysql()
            elif self.curDateTime - timedelta(hours=DEATH_PIG_AMENDMENT_PRIED) > death_time:
                if p1 > DEATH_ADJUST_RATE:
                    if DAY_TIME <= self.curDateTime.hour <= NIGHT_TIME:
                        self.death_type = 1
                        self.__param_insert_to_mysql(output_node='猪的死亡修正成功', p1=p1, p2=p2)
                        if self.to_mysql:
                            self.__death_update_to_mysql()
                    else:
                        self.__param_insert_to_mysql(output_node='不在白天修正时间段')
                else:
                    self.death_type = 3
                    self.__param_insert_to_mysql(output_node='猪的死亡修正失败', p1=p1, p2=p2)
                    if self.to_mysql:
                        self.__death_update_to_mysql()
            else:
                self.__param_insert_to_mysql(output_node='修正时间过短', p1=p1, p2=p2)

        elif p1 > DEATH_JUDGE_RATE:
            if self.curDateTime.hour < DAY_TIME or self.curDateTime.hour > NIGHT_TIME:
                self.death_type = 2
                self.__param_insert_to_mysql(output_node='进入猪的夜晚修正判别', p1=p1, p2=p2)
                if self.to_mysql:
                    self.__death_to_mysql()
            else:
                self.death_type = 1
                self.__param_insert_to_mysql(output_node='猪的死亡判别成功', p1=p1, p2=p2)
                if self.to_mysql:
                    self.__death_to_mysql()

        elif p1 > DEATH_ADJUST_RATE:
            self.death_type = 2
            self.__param_insert_to_mysql(output_node='进入猪的修正判别', p1=p1, p2=p2)
            if self.to_mysql:
                self.__death_to_mysql()
        else:
            self.__param_insert_to_mysql(output_node='不符合猪的死亡判别', p1=p1, p2=p2)

    @staticmethod
    def back_one_hot(y, rate=0.5):
        labels = []
        for i in y:
            label = 0
            if i[1] > rate:
                label = 1
            labels.append(label)
        return labels

    def __cow_death_judgement(self, backtrack_df):

        # 对于已确定死亡和判断死亡未处理的数据不做处理
        if str(self.markId) in self.have_death_markId_list:
            self.__param_insert_to_mysql(output_node='已确定死亡，不处理')
            return
        if str(self.markId) in self.unfirmed_markId_list:
            self.__param_insert_to_mysql(output_node='已标记死亡未处理')
            return
        if str(self.markId) not in self.normal_data_list:
            self.__param_insert_to_mysql(output_node='不属于正常数据')
            return

        # 提取数据
        new_df = backtrack_df[(backtrack_df['createTime'] < self.end_dt) & (
                backtrack_df['createTime'] >= self.end_dt - timedelta(hours=DEATH_COW_PERIOD))]  # 获取最新一段数据

        if len(new_df) == 0:
            if str(self.markId) in self.need_judge_death_cow_list:
                self.death_type = 1
                self.__death_update_to_mysql()
                self.__param_insert_to_mysql( output_node='数据缺失，牛的修正判别成功')
                return
            self.__param_insert_to_mysql(output_node='new_df数据缺失')
            return
        if len(new_df) < 20:
            self.__param_insert_to_mysql(output_node='new_df数据不足')
            return

        temp_cols = ['temperature_earTag', 'temperature_env']
        new_df[temp_cols] = new_df[temp_cols].fillna(method='ffill')
        new_df[temp_cols] = new_df[temp_cols].fillna(method='bfill')
        if new_df['temperature_env'].isnull().sum() == len(new_df):
            self.__param_insert_to_mysql(output_node='环境温度缺失')
            return

        feature = DeathModelFeature(new_df)
        sport_rate = feature.sport_rate(SPORT_THRESHOLD)
        temp_diff_rate = feature.temp_diff_rate(TEMP_DIFF_THRESHOLD)
        et1 = feature.mean_ear_temp()
        mt1 = feature.mean_env_temp()
        if str(self.markId) in self.need_judge_death_cow_list:
            query_death_time = "SELECT death_time FROM {} WHERE mark_id = {} ORDER BY death_time DESC LIMIT 1".format(
                DEATH_MODEL_TABLE, self.markId)
            death_time = pd.to_datetime(np.array(pd.read_sql(query_death_time, con=self.conn))[0][0])
            if self.curDateTime - timedelta(hours=DEATH_COW_AMENDMENT_PRIED) > death_time and DAY_TIME < self.curDateTime.hour < NIGHT_TIME:
                if sport_rate > 0.9 and temp_diff_rate > 0.9:
                    self.death_type = 1
                    self.__death_update_to_mysql()
                    self.__param_insert_to_mysql(sr1=sport_rate, dt1=temp_diff_rate, et1=et1, mt1=mt1, output_node='牛的修正判别成功')
                    return
                else:
                    self.death_type = 3
                    self.__death_update_to_mysql()
                    self.__param_insert_to_mysql(sr1=sport_rate, dt1=temp_diff_rate, et1=et1, mt1=mt1, output_node='牛的修正判别失败')
                    return
            else:
                self.__param_insert_to_mysql(sr1=sport_rate, dt1=temp_diff_rate, et1=et1, mt1=mt1, output_node='不在牛的修正时间段')
                return

        if sport_rate > 0.8 and temp_diff_rate > 0.8:
            self.death_type = 5
            self.__death_to_mysql()
            self.__param_insert_to_mysql(sr1=sport_rate, dt1=temp_diff_rate, et1=et1, mt1=mt1, output_node='牛的死亡判别成功')
            return
        else:
            self.__param_insert_to_mysql(sr1=sport_rate, dt1=temp_diff_rate, et1=et1, mt1=mt1, output_node='不符合牛的死亡判别')
            return

    def __death_to_mysql(self):
        if self.to_mysql is False:
            return
        # 写入死猪的数据sql语句
        death_to_sql = "INSERT INTO {} (`farm_id`,`mark_id`, `label_number`," \
                       "`type_conf_id`,`house_id`, `column_id`, `create_time`, `delete_flag`, " \
                       "`deal_flag`, `death_type`,`death_time`) value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                        DEATH_MODEL_TABLE)

        # 从animal_mask表中读取数据
        df = pd.read_sql("SELECT * FROM {} WHERE id={}".format(ANIMAL_MARK_TABLE, self.markId), con=self.conn)
        if len(df) != 1:
            logger.error('markId is the primary key but not the only one!')
        farm_id = df.at[0, 'farm_id']
        house_id = df.at[0, 'house_id']
        column_id = df.at[0, 'column_id']
        type_conf_id = df.at[0, 'type_conf_id']
        label_number = df.at[0, 'label_number']
        # 填充默认值
        delete_flag = 0
        deal_flag = 0
        # 构造写入标识
        self.cursor.execute(death_to_sql, (farm_id, self.markId, label_number, type_conf_id,
                                           house_id, column_id, self.curDateTime, delete_flag,
                                           deal_flag, self.death_type, self.curDateTime))
        # 提交到数据库执行
        self.conn.commit()

    def __death_update_to_mysql(self):
        if self.to_mysql is False:
            return
        update_to_sql = "UPDATE {} SET death_type=%s,create_time=%s WHERE mark_id = %s ORDER BY death_time DESC LIMIT 1".format(
            DEATH_MODEL_TABLE)
        self.cursor.execute(update_to_sql, (self.death_type, self.curDateTime, self.markId))
        self.conn.commit()

    def __param_insert_to_mysql(self, p1=None, p2=None, sr1=None, et1=None, mt1=None, dt1=None,
                                sr2=None, et2=None, mt2=None, dt2=None, fun=None, output_node=None, remark=None):

        parar_insert = "insert into {} (mark_id,label_number,create_time,update_time," \
                       "p1,p2,sr1,et1,mt1,dt1,sr2,et2,mt2,dt2,fun,output_node,remark) values" \
                       " (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )".format(
                        DEATH_PARAM_TABLE)
        self.cursor.execute(parar_insert,
                            (self.markId, self.label_number, self.curDateTime, datetime.now(),
                             p1, p2, sr1, et1, mt1, dt1, sr2, et2, mt2, dt2, fun, output_node, remark))
        self.conn.commit()

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()
