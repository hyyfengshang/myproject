# -*- coding:utf-8 -*-

"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 耳标状态模型
"""

import traceback
from tqdm import tqdm
from datetime import datetime, timedelta
import pymysql
from config import *
from param import *
import pandas as pd
import numpy as np
from log import logger
from report import send_error_report
from Featurization import EarTagModelFeature
import joblib
import tensorflow as tf


class EarTagStateModel(object):
    def __init__(self, to_mysql=True):
        # 1. 连接mysql
        self.conn = pymysql.connect(user=MYSQL_USER,
                                    password=MYSQL_PASSWORD,
                                    host=MYSQL_HOST,
                                    port=MYSQL_PORT
                                    )
        self.cursor = self.conn.cursor()
        self.cursor.execute("use animal4;")  # 调用生产环境的animal4数据库
        logger.info("Connect to MySQL successfully!")
        self.to_mysql = to_mysql

    def backtrack_for_detect(self, curDateTime, backtrack_data, backtrack_markIdList):
        """
        :param backtrack_markIdList: 回溯的markId
        :param backtrack_data: 回溯的数据
        :param curDateTime: 读取时间
        :return: 模型判别结果
        """
        self.curDateTime = curDateTime
        self.end_dt = datetime(year=curDateTime.year, month=curDateTime.month, day=curDateTime.day,
                               hour=curDateTime.hour, minute=curDateTime.minute, second=0)
        self.start_dt = self.end_dt - timedelta(hours=BACK_PERIOD)  # 回溯总的区间
        if len(backtrack_markIdList) == 0:
            logger.error("Backtrack markIdList is null")
            return
        if backtrack_data is None or len(backtrack_data) == 0:
            logger.error("Backtrack data is null")
            return
        new_data = backtrack_data[(backtrack_data['createTime'] < self.end_dt) & (
                backtrack_data['createTime'] >= self.end_dt - timedelta(hours=EMPTY_DATA_PERIOD))]  # 获取最后一段的数据
        # old_data = backtrack_data[(backtrack_data['createTime'] <= self.end_dt - timedelta(hours=TAG_OFF_PERIOD)) & (
        #         backtrack_data['createTime'] >= self.end_dt - timedelta(hours=TAG_OFF_PRIED + TAG_OFF_PERIOD))]  # 获取之前一段的数据
        # back_pried_data = backtrack_data[(backtrack_data['createTime'] < self.end_dt - timedelta(hours=TAG_OFF_PRIED + TAG_OFF_PERIOD))
        #                                  & (backtrack_data['createTime'] >= self.start_dt)]  # 获取最前一大段数据

        print('耳标状态判别模型')

        self.have_read_markId_list = new_data['markId'].unique()

        # 获得所有没有处理的耳标损坏和脱落的markId
        query_destroy_and_fall_off_ear_tang = "SELECT mark_id FROM {} WHERE (status = -1 or status = 2) and deal_flag = 0" \
            .format(EAR_TAG_MODEL_TABLE)
        destroy_and_ear_tag_off_data = np.array(
            pd.read_sql(query_destroy_and_fall_off_ear_tang, con=self.conn))
        self.destroy_and_ear_tag_off_list = [str(i[0]) for i in destroy_and_ear_tag_off_data]

        # 获得已处理的耳标对应的markId
        query_have_deal_ear_tang = "SELECT mark_id FROM {} WHERE deal_flag = 1".format(EAR_TAG_MODEL_TABLE)
        have_deal_ear_tang_data = np.array(pd.read_sql(query_have_deal_ear_tang, con=self.conn))
        self.have_deal_ear_tang_list = [str(i[0]) for i in have_deal_ear_tang_data]

        # 查询无效耳标的markId
        query_invalid_earplug = "SELECT mark_id FROM {} WHERE status = 2 or status=-1 or status= 0 ".format(
            EAR_TAG_MODEL_TABLE)
        invalid_earplug_markId = np.array(pd.read_sql(query_invalid_earplug, con=self.conn))
        self.invalid_earplug_markId_list = [str(i[0]) for i in invalid_earplug_markId]

        # 获得所有未佩戴的markId
        query_not_wear_data = "SELECT mark_id FROM {} WHERE status=0 and deal_flag = 0".format(EAR_TAG_MODEL_TABLE)
        not_wear_data = np.array(pd.read_sql(query_not_wear_data, con=self.conn))
        self.not_wear_data_list = [str(i[0]) for i in not_wear_data]

        # 查询已写入的未处理数据，避免重复写入
        have_insert_ear_tang = "SELECT mark_id FROM {} WHERE deal_flag = 0".format(EAR_TAG_MODEL_TABLE)
        have_insert_tang_makId = np.array(pd.read_sql(have_insert_ear_tang, con=self.conn))
        self.have_insert_tang_makId_list = [str(i[0]) for i in have_insert_tang_makId]

        have_insert_realtime_data = "SELECT mark_id FROM {}".format(ANIMAL_REALTIME_TABLE)
        have_insert_realtime = np.array(pd.read_sql(have_insert_realtime_data, con=self.conn))
        self.have_insert_realtime_list = [str(i[0]) for i in have_insert_realtime]

        # 获得所有确定的死猪的markId
        query_have_death_markId = "SELECT mark_id FROM {} WHERE pre_audit_status = 1".format(DEATH_MODEL_TABLE)
        have_death_markId = np.array(pd.read_sql(query_have_death_markId, con=self.conn))
        self.have_death_markId_list = [str(i[0]) for i in have_death_markId]

        # 获得所有未确定的死猪的markId
        query_unconfirmed_markId = "SELECT mark_id FROM {} WHERE pre_audit_status = 0 and (death_type = 1 or death_type = 2)".format(
            DEATH_MODEL_TABLE)
        unfirmed_markId = np.array(pd.read_sql(query_unconfirmed_markId, con=self.conn))
        self.unfirmed_markId_list = [str(i[0]) for i in unfirmed_markId]

        # 获得需要进行死亡修正判别的markId
        need_judge_death_data = "SELECT mark_id FROM {} WHERE death_type = 2 or death_type = 4 or death_type = 5".format(
            DEATH_MODEL_TABLE)
        need_judge_death = np.array(pd.read_sql(need_judge_death_data, con=self.conn))
        self.need_judge_death_list = [str(i[0]) for i in need_judge_death]

        # 获得脱落待修正的markId
        need_judge_tag_off_data = "SELECT mark_id FROM {} WHERE status = 20".format(EAR_TAG_UNDETERMINED_TABLE)
        need_judge_tag_off = np.array(pd.read_sql(need_judge_tag_off_data, con=self.conn))
        self.need_judge_tag_off_list = [str(i[0]) for i in need_judge_tag_off]

        # 获得特殊脱落待定的markId
        undetermined_tag_off_data = "SELECT mark_id FROM {} WHERE status = 21".format(EAR_TAG_MODEL_TABLE)
        undetermined_tag_off = np.array(pd.read_sql(undetermined_tag_off_data, con=self.conn))
        self.undetermined_tag_off_list = [str(i[0]) for i in undetermined_tag_off]

        # 获得所有正常数据的markId
        query_normal_data = "SELECT mark_id FROM {} WHERE status = 1 and deal_flag = 0".format(EAR_TAG_MODEL_TABLE)
        normal_data = np.array(pd.read_sql(query_normal_data, con=self.conn))
        self.normal_data_list = [str(i[0]) for i in normal_data]

        self.tag_off_model = tf.keras.models.load_model(TAG_OFF_FILE)
        self.tag_off_scaler = joblib.load(TAG_OFF_SCALER_FILE)

        for markId in tqdm(np.unique(backtrack_markIdList)):
            self.markId = markId
            self.label_number = None
            self.status_remark = None
            self.ai_remark = None
            query_label_number = "SELECT label_number FROM {} WHERE id = {}".format(ANIMAL_MARK_TABLE, self.markId)
            df = pd.read_sql(query_label_number, con=self.conn)
            if len(df) == 0:
                self.__param_insert_to_mysql(output_node='耳标不存在')
                continue
            self.label_number = np.array(df)[0][0]
            try:
                self.__ear_tag_judgement(backtrack_data)
            except:
                logger.error('{} Run in ear tang model filed!'.format(self.label_number))
                send_error_report(self.label_number, traceback.format_exc())
            else:
                pass

    def __ear_tag_judgement(self, backtrack_data):
        backtrack_df = backtrack_data[backtrack_data['markId'] == self.markId].copy()
        query_type_conf_id = "SELECT type_conf_id FROM {} WHERE id = {}".format(ANIMAL_MARK_TABLE, self.markId)
        df = pd.read_sql(query_type_conf_id, con=self.conn)
        if len(df) == 0:
            self.__param_insert_to_mysql(output_node='种类不存在')
            return
        self.type_conf_id = np.array(df)[0][0]

        # 脱落修正模型
        if str(self.markId) in self.need_judge_tag_off_list:
            self.__tag_off_amendment(backtrack_data)
            return

        # 对于已经标记损坏和脱落的数据直接不做处理，加快模型运行效率
        if str(self.markId) in self.destroy_and_ear_tag_off_list:
            self.__param_insert_to_mysql(output_node='已损坏或者脱落，不处理')
            return
        # 对于已确定死亡和判断死亡未处理的数据也不做处理
        if str(self.markId) in self.have_death_markId_list:
            self.__param_insert_to_mysql(output_node='已确定死亡，不处理')
            return
        if str(self.markId) in self.unfirmed_markId_list:
            self.__param_insert_to_mysql(output_node='已标记死亡未处理')
            return
        if str(self.markId) in self.need_judge_death_list:
            self.__param_insert_to_mysql(output_node='已标记为死亡待修正')
            return
        # 未读取到数据的判别
        if self.markId not in self.have_read_markId_list:
            self.ai_remark = -2
            self.status = -2
            self.__param_insert_to_mysql(remark=-2, output_node='空数据判别')
            self.__ear_tag_to_mysql()
            return

        # 提取数据
        new_df = backtrack_df.tail(TAG_OFF_PERIOD * 40)
        new_df.reset_index(inplace=True, drop=True)  # 获取最后一段的数据

        if len(new_df) == 0:
            self.__param_insert_to_mysql(output_node='new_df数据缺失')
            return
        # 耳标损坏特征工程
        temp_max = np.max(new_df['temperature_earTag'])
        temp_min = np.min(new_df['temperature_earTag'])
        sport_max = np.max(new_df['xyz_axis_abs_sum'])
        # 耳标损坏判别
        if temp_max is None:  # 耳标损坏
            self.ai_remark = -1
            self.status = -1
            self.status_remark = '温度传感器损坏'
            self.__param_insert_to_mysql(sport_max=sport_max, temp_max=temp_max, remark=-1, output_node='损坏判别1')
            return
        if temp_max > TEMP_MAX_THRESHOLD or temp_min < TEMP_MIN_THRESHOLD:  # 耳标损坏
            if str(self.markId) in self.not_wear_data_list:
                self.__param_insert_to_mysql(temp_max=temp_max, temp_min=temp_min, sport_max=sport_max,
                                             output_node='未佩戴数据，损坏判别剔除')
                return
            self.ai_remark = -1
            self.status = -1
            self.status_remark = '温度传感器损坏'
            self.__ear_tag_to_mysql()
            self.__param_insert_to_mysql(temp_max=temp_max, temp_min=temp_min, sport_max=sport_max, remark=-1,
                                         output_node='损坏判别2')
            return

        if len(new_df) < 5:
            self.__param_insert_to_mysql(output_node='new_df数据不足')
            return

        old_df = backtrack_df.tail(TAG_OFF_BACK_PRIED * 60)  # 获取脱落前一大段数据
        old_df.reset_index(inplace=True, drop=True)

        back_pried_df = backtrack_df.head(TAG_OFF_BACK_PRIED * 60)  # 获取最前一大段数据
        back_pried_df.reset_index(inplace=True, drop=True)

        back_ago_df = backtrack_df[(backtrack_df['createTime'] < self.start_dt + timedelta(hours=0.5)) & (
                backtrack_df['createTime'] >= self.start_dt)]  # 获取最前一小段数据
        back_ago_df.reset_index(inplace=True, drop=True)

        # 耳标脱落特征工程
        temp_cols = ['temperature_earTag', 'temperature_env']
        new_df[temp_cols] = new_df[temp_cols].fillna(method='ffill')  # 空值填充
        new_df[temp_cols] = new_df[temp_cols].fillna(method='bfill')
        tag_off_feature = EarTagModelFeature(new_df, old_df, back_pried_df)
        mean_env_temp = np.mean(new_df['temperature_env'])  # 环境温度均值

        mean_ear_temp = np.mean(back_pried_df['temperature_earTag'])  # 正常体温均值
        mean_ear_temp = np.where(mean_ear_temp > MINIMUM_EAR_TEMP, mean_ear_temp, MINIMUM_EAR_TEMP)
        if len(back_pried_df) == 0:
            mean_ear_temp = DEFAULT_EAR_TEMPERATURE  # 无法获取正常体温时设定默认正常体温
        if np.isnan(mean_env_temp):  # 环境温度为空时,跳出判别
            self.__param_insert_to_mysql(output_node='环境温度为空，无法判别')
            return

        tag_off_judge = self.__tag_off_judgement(backtrack_data)
        if tag_off_judge == 0:
            return

        sport_rate = tag_off_feature.sport_rate(SPORT_THRESHOLD)
        diff_rate = tag_off_feature.diff_rate(DELTA_TEMP_THRESHOLD)

        # 耳标脱落和未佩戴判别
        if sport_rate > SPORT_RATE_THRESHOLD:  # 如果运动量较低
            # 如果为未佩戴数据，更新时间，直接返回
            if str(self.markId) in self.not_wear_data_list:
                self.ai_remark = 0
                self.status = 0
                self.__ear_tag_to_mysql()
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, remark=0,
                    output_node='更新未佩戴时间')
                return

            back_sport_number = np.sum(backtrack_df['xyz_axis_abs_sum'].values < 15)
            back_sport_rate = back_sport_number / len(backtrack_df)  # 总区间运动占比
            # back_ear_temp = np.mean(back_pried_df['temperature_earTag'])  # 总区间耳标温度均值
            # back_env_temp = np.mean(back_pried_df['temperature_env'])   # 总区间环境温度均值
            # back_temp_diff = back_ear_temp - back_env_temp    # 总区间温度差均值
            # 如果一天的运动量都很低，并且温度接近环境温度，判定为未佩戴
            if back_sport_rate > 0.9 and diff_rate > TEMP_DIFF_RATE_THRESHOLD:
                self.ai_remark = 0
                self.status = 0
                self.__ear_tag_to_mysql()
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, remark=0,
                    back_sport_rate=back_sport_rate, output_node='未佩戴1')
                return
            # 如果运动量为0的占比过多，表示运动传感器损坏
            back_sport_zero = np.sum(backtrack_df['xyz_axis_abs_sum'].values == 0) / len(backtrack_df)
            if back_sport_zero > 0.98:
                if str(self.markId) in self.not_wear_data_list:
                    self.__param_insert_to_mysql(output_node='未佩戴数据，运动传感器损坏剔除')
                    return
                if len(backtrack_df) < 10 * BACK_PERIOD:
                    self.__param_insert_to_mysql(output_node='回溯的数据量过少,不足以判断运动传感器损坏')
                    return
                self.ai_remark = -1
                self.status = -1
                self.status_remark = '运动传感器损坏'
                self.__ear_tag_to_mysql()
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, remark=-1,
                    back_sport_rate=back_sport_rate, output_node='运动传感器损坏1')
                return
            # 剩余数据进行正常数据判别
            else:
                # 剔除损坏数据后，剔除环境温度影响的未佩戴数据判别
                if back_sport_rate > 0.9:
                    self.ai_remark = 0
                    self.status = 0
                    self.__ear_tag_to_mysql()
                    self.__param_insert_to_mysql(
                        sport_max=sport_max,
                        temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                        temp_min=temp_min,
                        mean_env_temp=mean_env_temp, remark=0,
                        back_sport_rate=back_sport_rate, output_node='未佩戴2')
                    return
                # # 对于刚录入的数据，避免运动量影响造成误判为正常数据
                elif len(back_ago_df) == 0:  # v3.5.1添加，用于剔除刚录入数据
                    self.__param_insert_to_mysql(
                        sport_max=sport_max,
                        temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                        temp_min=temp_min,
                        mean_env_temp=mean_env_temp, temp_mean=mean_ear_temp, remark=0,
                        back_sport_rate=back_sport_rate, output_node='回溯最开始数据为空，不做处理1')
                    return
                # 剩余数据判断为正常数据
                else:
                    self.ai_remark = 1
                    self.status = 1
                    self.__ear_tag_to_mysql()
                    self.__param_insert_to_mysql(
                        sport_max=sport_max,
                        temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                        temp_min=temp_min,
                        mean_env_temp=mean_env_temp, remark=1,
                        back_sport_rate=back_sport_rate, output_node='正常数据1')
                    return

        # 其他数据进行正常数据判别
        else:
            back_sport_number = np.sum(backtrack_df['xyz_axis_abs_sum'].values < 20)
            back_sport_rate = back_sport_number / len(backtrack_df)
            # 如果一天的运动量都很低，判定为未佩戴
            if back_sport_rate > 0.9:
                self.ai_remark = 0
                self.status = 0
                self.__ear_tag_to_mysql()
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, temp_mean=mean_ear_temp, remark=0,
                    back_sport_rate=back_sport_rate, output_node='未佩戴3')
                return
            # # 如果回溯最开始的数据为空，不做处理， 对于刚录入的数据，避免运动量影响造成误判为正常数据
            if len(back_ago_df) == 0:  # v3.5.1添加，用于剔除刚录入数据
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, temp_mean=mean_ear_temp, remark=0,
                    back_sport_rate=back_sport_rate, output_node='回溯最开始数据为空，不做处理2')
                return
            # 剩余数据判断为正常数据
            else:
                self.ai_remark = 1
                self.status = 1
                self.__ear_tag_to_mysql()
                self.__param_insert_to_mysql(
                    sport_max=sport_max,
                    temp_diff=diff_rate, sport_rate=sport_rate, temp_max=temp_max,
                    temp_min=temp_min,
                    mean_env_temp=mean_env_temp, temp_mean=mean_ear_temp, remark=1,
                    back_sport_rate=back_sport_rate, output_node='正常数据2')
                return

    def __tag_off_judgement(self, backtrack_data):

        if str(self.markId) in self.not_wear_data_list:
            self.__param_insert_to_mysql(output_node='未佩戴数据，脱落判别剔除')
            return 1

        backtrack_df = backtrack_data[backtrack_data['markId'] == self.markId].copy()
        df = backtrack_df.tail(TAG_OFF_SEQ_LEN)
        df.reset_index(inplace=True, drop=True)

        if len(df) < TAG_OFF_SEQ_LEN:
            self.__param_insert_to_mysql(output_node='数据不足，不作处理')
            return 1
        df['xAxis'] = np.abs(df['xAxis'])
        df['yAxis'] = np.abs(df['yAxis'])
        df['zAxis'] = np.abs(df['zAxis'])
        df['temperature_env'] = df['temperature_env'].fillna(method='ffill')
        df['temperature_env'] = df['temperature_env'].fillna(method='bfill')
        if df['temperature_env'].isnull().sum() == len(df):
            df['temperature_env'] = 22
        df['temperature_diff'] = df['temperature_earTag'] - df['temperature_env']
        predict_cols = ['temperature_earTag', 'temperature_env', 'temperature_diff', 'xAxis', 'yAxis', 'zAxis']
        predict_df = np.array(df[predict_cols])
        x = self.tag_off_scaler.transform(predict_df.reshape(-1, 1)).reshape(-1, TAG_OFF_SEQ_LEN, N_CHANNELS)
        x = np.expand_dims(x, axis=1)
        out = np.array(self.tag_off_model(x))
        p0 = out[0][0]
        p1 = out[0][1]
        p2 = out[0][2]
        p3 = out[0][3]
        result = np.argmax(out[0])
        # if p1 > TAG_OFF_JUDGE_RATE:
        #     # if self.curDateTime.hour < DAY_TIME or self.curDateTime.hour > NIGHT_TIME:
        #     #     self.status = 20
        #     #     self.__param_insert_to_mysql(output_node='进入夜晚脱落修正判别', p0=p0,  p1=p1, p2=p2, p3=p3)
        #     #     if self.to_mysql:
        #     #         self.__ear_tag_to_mysql()
        #     # else:
        #     self.status = 2
        #     self.__param_insert_to_mysql(output_node='脱落判别成功', p0=p0, p1=p1, p2=p2, p3=p3, remark=2)
        #     if self.to_mysql:
        #         self.__ear_tag_to_mysql()
        #     return 0

        if result == 1:
            self.status = 20
            self.__param_insert_to_mysql(output_node='符合脱落特征，进入脱落修正判别', p0=p0, p1=p1, p2=p2, p3=p3, remark=2)
            if self.to_mysql:
                self.__ear_tag_to_undetermined_mysql()
            return 0
        elif result == 3:
            tag_off_feature = EarTagModelFeature(old_df=df)
            max_temp_change = tag_off_feature.maximum_rate_of_temp_change()  # V3.5.1添加
            if max_temp_change > MAX_TEMP_CHANGE_THRESHOLD:
                self.status = 20
                self.__param_insert_to_mysql(output_node='达到温度变化阈值，进入脱落修正判别', p0=p0, p1=p1, p2=p2, p3=p3,
                                             max_temp_change=max_temp_change, remark=2)
                if self.to_mysql:
                    self.__ear_tag_to_undetermined_mysql()
                return 0
            else:
                self.__param_insert_to_mysql(output_node='未达到温度变化阈值，不符合脱落判别', p0=p0, p1=p1, p2=p2, p3=p3,
                                             max_temp_change=max_temp_change)
                return 1
        else:
            self.__param_insert_to_mysql(output_node='不符合脱落判别', p0=p0, p1=p1, p2=p2, p3=p3)
            return 1

    def __tag_off_amendment(self, backtrack_data):
        backtrack_df = backtrack_data[backtrack_data['markId'] == self.markId].copy()
        df = backtrack_df.tail(TAG_OFF_SEQ_LEN)
        df.reset_index(inplace=True, drop=True)
        if len(df) == 0:
            self.status = 2
            self.__param_insert_to_mysql(output_node='数据缺失,脱落修正成功')
            if self.to_mysql:
                self.__ear_tag_update_to_mysql()
                self.__ear_tag_update_to_undetermined_mysql()
            return 0

        if len(df) < TAG_OFF_SEQ_LEN:
            self.__param_insert_to_mysql(output_node='数据不足，不作处理')
            return 0
        df['xAxis'] = np.abs(df['xAxis'])
        df['yAxis'] = np.abs(df['yAxis'])
        df['zAxis'] = np.abs(df['zAxis'])
        df['temperature_env'] = df['temperature_env'].fillna(method='ffill')
        df['temperature_env'] = df['temperature_env'].fillna(method='bfill')
        if df['temperature_env'].isnull().sum() == len(df):
            df['temperature_env'] = 22
        df['temperature_diff'] = df['temperature_earTag'] - df['temperature_env']
        predict_cols = ['temperature_earTag', 'temperature_env', 'temperature_diff', 'xAxis', 'yAxis', 'zAxis']
        predict_df = np.array(df[predict_cols])
        x = self.tag_off_scaler.transform(predict_df.reshape(-1, 1)).reshape(-1, TAG_OFF_SEQ_LEN, N_CHANNELS)
        x = np.expand_dims(x, axis=1)
        out = np.array(self.tag_off_model(x))
        p0 = out[0][0]
        p1 = out[0][1]
        p2 = out[0][2]
        p3 = out[0][3]
        result = np.argmax(out[0])
        query_update_time = "SELECT update_time FROM {} WHERE mark_id = {} ORDER BY update_time DESC LIMIT 1".format(
            EAR_TAG_UNDETERMINED_TABLE, self.markId)
        update_time = pd.to_datetime(np.array(pd.read_sql(query_update_time, con=self.conn))[0][0])  # 获取待定数据的最后更新时间
        if result == 1 or result == 3:
            self.__param_insert_to_mysql(output_node='符合脱落特征，继续修正', p0=p0, p1=p1, p2=p2, p3=p3)
            return 0
        elif result == 2 and DAY_TIME <= self.curDateTime.hour <= NIGHT_TIME:
            self.status = 2
            self.__param_insert_to_mysql(output_node='符合脱落修正，跳出修正', p0=p0, p1=p1, p2=p2, p3=p3)
            if self.to_mysql:
                self.__ear_tag_update_to_mysql()
                self.__ear_tag_update_to_undetermined_mysql()
            return 0
        elif self.curDateTime - timedelta(hours=TAG_OFF_AMENDMENT_PRIED) > update_time:
            if result == 2:
                if DAY_TIME <= self.curDateTime.hour <= NIGHT_TIME:
                    self.status = 2
                    self.__param_insert_to_mysql(output_node='脱落修正成功', p0=p0, p1=p1, p2=p2, p3=p3)
                    if self.to_mysql:
                        self.__ear_tag_update_to_mysql()
                        self.__ear_tag_update_to_undetermined_mysql()
                    return 0
                else:
                    self.__param_insert_to_mysql(output_node='不在白天修正时间段', p0=p0, p1=p1, p2=p2, p3=p3)
                    return 0
            else:
                self.status = 1
                self.__param_insert_to_mysql(output_node='脱落修正失败', p0=p0, p1=p1, p2=p2, p3=p3)
                if self.to_mysql:
                    self.__ear_tag_update_to_mysql()
                    self.__ear_tag_update_to_undetermined_mysql()
                return 0
        else:
            self.__param_insert_to_mysql(output_node='修正时间过短', p0=p0, p1=p1, p2=p2, p3=p3)
            return 0

    def __param_insert_to_mysql(self, temp_diff=None, sport_rate=None, temp_change=None, temp_threshold=None,
                                temp_mean=None, p0=None, p1=None, p2=None, p3=None,
                                back_sport_rate=None, temp_max=None, temp_min=None, sport_max=None, mean_env_temp=None,
                                max_temp_change=None, output_node=None, remark=None):
        """参数记录，用于保存模型运行参数，方便查看模型运行情况"""
        parar_insert = "insert into {} (mark_id,label_number,create_time,update_time," \
                       "temp_diff,sport_rate,temp_change,temp_threshold,temp_mean," \
                       "back_sport_rate,temp_max,temp_min,sport_max,mean_env_temp," \
                       "max_temp_change,output_node,remark,p0,p1,p2,p3) values" \
                       " (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s ,%s)".format(
                        EAR_TAG_PARAM_TABLE)

        try:
            self.cursor.execute(parar_insert,
                                (self.markId, self.label_number, self.curDateTime, datetime.now(),
                                 temp_diff, sport_rate, temp_change, temp_threshold, temp_mean,
                                 back_sport_rate, temp_max, temp_min, sport_max, mean_env_temp,
                                 max_temp_change, output_node, remark, p0, p1, p2, p3))

            self.conn.commit()

        except Exception as e:
            traceback.print_exc()
            self.conn.rollback()
            logger.error("Mysql param > {} insert ear tag failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql param > insert ear tag successfully！")

    def __ear_tag_insert_to_mysql(self):
        """将耳标状态写入device_alarm表中"""
        # 写入耳标状态sql语句
        ear_tag_to_sql = "insert into {} (mark_id,status,status_remark,batch_id,insert_time,device_type,label_number,ai_remark,update_time )" \
                         " values (%s, %s, %s, %s, %s , %s, %s, %s, %s)".format(EAR_TAG_MODEL_TABLE)

        try:
            batch_id = int('{0:%Y%m%d%H%M%S%f}'.format(datetime.now())[2:])
            self.cursor.execute(ear_tag_to_sql,
                                (self.markId, self.status, self.status_remark, batch_id, self.curDateTime, 1,
                                 self.label_number,
                                 self.ai_remark, self.curDateTime))
            # 提交到数据库执行
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql device_alarm > {} insert ear tag failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql device_alarm > insert ear tag successfully！")

    def __ear_tag_update_to_mysql(self):
        """更新耳标状态到device_alarm表中"""
        update_device_alarm = "UPDATE {} SET status=%s,status_remark=%s,update_time=%s,label_number=%s WHERE mark_id=%s and deal_flag = 0 ORDER BY insert_time DESC LIMIT 1".format(
            EAR_TAG_MODEL_TABLE)

        try:
            self.cursor.execute(update_device_alarm,
                                (self.status, self.status_remark, self.curDateTime, self.label_number, self.markId))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql device_alarm > {} update ear tag failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql device_alarm > update ear tag successfully！")

    def __tag_on_insert_to_mysql(self):
        """将耳标状态写入animal_realtime表中"""
        animal_realtime_sql = "insert into {} (mark_id, health_score, tag_on,create_time,update_time) values(%s,%s,%s,%s,%s)".format(
            ANIMAL_REALTIME_TABLE)
        try:
            self.cursor.execute(animal_realtime_sql, (self.markId, -1, self.status, self.curDateTime, self.curDateTime))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql animal_realtime > {} insert tag on failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql animal_realtime > insert tag on successfully！")

    def __tag_on_update_to_mysql(self):
        """更新耳标状态到animal_realtime表中"""
        update_animal_realtime = "UPDATE {} SET tag_on=%s,update_time=%s WHERE mark_id=%s ".format(
            ANIMAL_REALTIME_TABLE)
        try:
            self.cursor.execute(update_animal_realtime, (self.status, self.curDateTime, self.markId))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql animal_realtime > {} update tag on failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql animal_realtime > update tag on successfully！")

    def __ear_tag_deal_to_mysql(self):
        """保留处理记录并插入处理后新的信息"""
        insert_device_alarm = "insert into {} (mark_id,status,status_remark,batch_id,insert_time,device_type,label_number,ai_remark,update_time )" \
                              " values (%s, %s, %s, %s, %s ,%s ,%s ,%s, %s)".format(EAR_TAG_MODEL_TABLE)

        update_animal_realtime = "UPDATE {} SET tag_on=%s,update_time=%s WHERE mark_id=%s ".format(
            ANIMAL_REALTIME_TABLE)

        try:
            batch_id = int('{0:%Y%m%d%H%M%S%f}'.format(datetime.now())[2:])
            self.cursor.execute(insert_device_alarm,
                                (self.markId, self.status, self.status_remark, batch_id, self.curDateTime,
                                 1, self.label_number, self.ai_remark, self.curDateTime))
            self.cursor.execute(update_animal_realtime, (self.status, self.curDateTime, self.markId))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql device alarm and animal realtime > {} insert and update ear tag failed: {}！".format(
                self.label_number, e))
        else:
            logger.info("Mysql device alarm and animal realtime > insert and update ear tag successfully！")

    def __ear_tag_to_mysql(self):
        """将耳标状态分别写入device_alarm表和animal_realtime表中"""
        # 对于已经写入并且未处理的耳标进行更新
        if str(self.markId) in self.have_insert_tang_makId_list:
            self.__ear_tag_update_to_mysql()
        # 未写入的插入
        else:
            # 对于已经处理的耳标进行插入和更新
            if str(self.markId) in self.have_deal_ear_tang_list:
                self.__ear_tag_deal_to_mysql()
            else:
                self.__ear_tag_insert_to_mysql()  # 对于不存在的数据做插入
        # 同上
        if str(self.markId) in self.have_insert_realtime_list:
            self.__tag_on_update_to_mysql()
        else:
            self.__tag_on_insert_to_mysql()

    def __ear_tag_to_undetermined_mysql(self):

        ear_tag_to_sql = "insert into {} (mark_id,label_number,status,update_time )" \
                         " values (%s, %s, %s, %s)".format(EAR_TAG_UNDETERMINED_TABLE)
        try:
            self.cursor.execute(ear_tag_to_sql, (self.markId, self.label_number, self.status, self.curDateTime))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql device alarm undetermined > {} insert ear tag failed: {}！".format(
                self.label_number, e))
        else:
            logger.info("Mysql device alarm undetermined> insert ear tag successfully！")

    def __ear_tag_update_to_undetermined_mysql(self):
        update_device_alarm = "UPDATE {} SET status=%s,update_time=%s,label_number=%s WHERE mark_id=%s ORDER BY update_time DESC LIMIT 1".format(
            EAR_TAG_UNDETERMINED_TABLE)

        try:
            self.cursor.execute(update_device_alarm,
                                (self.status, self.curDateTime, self.label_number, self.markId))
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(e)
            self.conn.rollback()
            logger.error("Mysql device_alarm undetermined > {} update ear tag failed: {}！".format(self.label_number, e))
        else:
            logger.info("Mysql device_alarm undetermined > update ear tag successfully！")

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()
