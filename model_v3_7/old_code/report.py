"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 耳标状态和死亡判别模型
"""
from email.mime.multipart import MIMEMultipart
import pymysql
from log import logger
from old_code.configuration import *
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header


class Report(object):
    def __init__(self, write=True, send=False):
        # 1. 连接mysql
        self.conn = pymysql.connect(user=MYSQL_USER,
                                    password=MYSQL_PASSWORD,
                                    host=MYSQL_HOST,
                                    port=MYSQL_PORT
                                    )
        self.cursor = self.conn.cursor()
        self.cursor.execute("use animal4;")  # 调用生产环境的animal4数据库
        logger.info("Connect to MySQL successfully!")
        self.write = write
        self.send = send

    def day_report(self, curDateTime):
        self.__DeathReport(curDateTime=curDateTime, back_time=1, saveXpath='./report/day_report',
                           mold='Daily', write=self.write, send_email=self.send)

    def weak_report(self, curDateTime):
        self.__DeathReport(curDateTime=curDateTime, back_time=7, saveXpath='./report/weak_report',
                           mold='Weakly', write=self.write, send_email=self.send)

    def month_report(self, curDateTime):
        self.__DeathReport(curDateTime=curDateTime, back_time=30, saveXpath='./report/month_report',
                           mold='Monthly', write=self.write, send_email=self.send)

    def all_report(self, curDateTime):
        self.day_report(curDateTime)
        self.weak_report(curDateTime)
        self.month_report(curDateTime)

    def __DataGet(self, curDateTime, back_time):
        assert isinstance(curDateTime, datetime)
        end_dt = curDateTime.strftime('%Y%m%d%H%M%S')
        start_dt = (curDateTime - timedelta(days=back_time)).strftime('%Y%m%d%H%M%S')

        query_all_death_data = "SELECT label_number,death_time,B.name,place_name,D.name FROM " \
                               "{} A,sys_farm B,animal_farm_place C,conf_type_info  D  " \
                               "WHERE death_type = 1 and death_time>{} " \
                               "and death_time<{} AND A.label_number LIKE '10000010%' AND A.farm_id= B.id " \
                               "AND A.farm_id = C.farm_id AND A.column_id = C.id AND A.type_conf_id=D.id" \
                               " ORDER BY A.death_time". \
            format(DEATH_MODEL_TABLE, start_dt, end_dt)
        all_death_data = np.array(pd.read_sql(query_all_death_data, con=self.conn))

        query_all_tag_off_data = "SELECT A.label_number,A.update_time,B.name,place_name,D.name FROM " \
                                 "{} A,sys_farm B,animal_farm_place C,conf_type_info D,animal_mark E " \
                                 "WHERE A.status = 2 AND A.update_time > {} AND A.update_time < {} AND " \
                                 "A.label_number LIKE '10000010%' AND A.mark_id =E.id AND " \
                                 "E.farm_id= B.id AND E.type_conf_id=D.id AND E.farm_id = C.farm_id " \
                                 "AND E.column_id = C.id ORDER BY A.update_time".format(EAR_TAG_MODEL_TABLE, start_dt, end_dt)
        all_tang_off_data = np.array(pd.read_sql(query_all_tag_off_data, con=self.conn))

        query_all_tag_damage_data = "SELECT A.label_number,A.update_time,B.name,place_name,D.name FROM " \
                                    "{} A,sys_farm B,animal_farm_place C,conf_type_info D,animal_mark E " \
                                    "WHERE A.status = -1 AND A.update_time > {} AND A.update_time < {} AND " \
                                    "A.label_number LIKE '10000010%' AND A.mark_id =E.id AND " \
                                    "E.farm_id= B.id AND E.type_conf_id=D.id AND E.farm_id = C.farm_id " \
                                    "AND E.column_id = C.id ORDER BY A.update_time".format(EAR_TAG_MODEL_TABLE, start_dt, end_dt)
        all_tag_damage_data = np.array(pd.read_sql(query_all_tag_damage_data, con=self.conn))

        data = [all_death_data, all_tang_off_data, all_tag_damage_data]

        return data

    # 写入报告文件
    @staticmethod
    def __WriteReport(curDateTime, saveXpath, mold, data_list):
        assert isinstance(curDateTime, datetime)
        date = curDateTime.strftime('%Y%m%d')
        Death_path = saveXpath + '/' + str(date) + '_Death_%s_Report.txt' % mold
        Tag_off_path = saveXpath + '/' + str(date) + '_Tag_off_%s_Report.txt' % mold
        Tag_damage_path = saveXpath + '/' + str(date) + '_Tag_damage_%s_Report.txt' % mold
        # 死亡数据
        f1 = open(Death_path, 'a')
        f1.write('死亡数据' + '\n')
        for judge_data in data_list[0]:
            for data in judge_data:
                f1.write(str(data) + '  ')
            f1.write('\n')
        f1.close()
        # 脱落数据
        f2 = open(Tag_off_path, 'a')
        f2.write('脱落数据' + '\n')
        for judge_data in data_list[1]:
            for data in judge_data:
                f2.write(str(data) + '  ')
            f2.write('\n')
        f2.close()
        # 损坏数据
        f3 = open(Tag_damage_path, 'a')
        f3.write('损坏数据' + '\n')
        for judge_data in data_list[2]:
            for data in judge_data:
                f3.write(str(data) + '  ')
            f3.write('\n')
        f3.close()

        return [Death_path, Tag_off_path, Tag_damage_path]

    @staticmethod
    def __SendReport(mold, flieXpath, curDateTime, back_time):
        assert isinstance(curDateTime, datetime)
        end_dt = curDateTime.strftime('%Y%m%d')
        start_dt = (curDateTime - timedelta(days=back_time)).strftime('%Y%m%d')

        smtp_server = SMTP_SERVER
        from_addr = FROM_ADDRESS
        password = PASS_WORD
        to_addr = TO_ADDRESS
        server = smtplib.SMTP_SSL(smtp_server, 465)
        msg = MIMEMultipart()
        msg['Subject'] = Header('EarTag AND Death Model %s Report %s——%s' % (mold, start_dt, end_dt), 'utf-8')
        msg['From'] = Header(from_addr, 'utf-8')
        msg['To'] = Header(to_addr[0], 'utf-8')
        f1 = open(flieXpath[0])
        death_data = f1.read()
        f1.close()
        f2 = open(flieXpath[1])
        tag_off_data = f2.read()
        f2.close()
        f3 = open(flieXpath[2])
        tag_damage_data = f3.read()
        f3.close()

        msg.attach(MIMEText("{}\n\n".format(death_data)))
        msg.attach(MIMEText("{}\n\n".format(tag_off_data)))
        msg.attach(MIMEText("{}\n\n".format(tag_damage_data)))

        # # 构造附件1
        # att1 = MIMEText(open(flieXpath, 'rb').read(), 'base64', 'utf-8')
        # att1['Content-Type'] = 'application/octet-stream'
        # att1["Content-Disposition"] = 'attachment; filename=%s' % flieXpath
        # msg.attach(att1)

        server.login(from_addr, password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        print("Successfully sent %s report email" % mold)
        server.quit()

    def __DeathReport(self, curDateTime, back_time, saveXpath, mold, write=False, send_email=False):
        if not os.path.exists(saveXpath):
            os.makedirs(saveXpath)
        data_list = self.__DataGet(curDateTime, back_time)
        if write:
            fileXpath_list = self.__WriteReport(curDateTime, saveXpath, mold, data_list)
            if send_email:
                self.__SendReport(mold, fileXpath_list, curDateTime, back_time)


def send_error_report(model, error):
    smtp_server = SMTP_SERVER
    from_addr = FROM_ADDRESS
    password = PASS_WORD
    to_addr = TO_ADDRESS_ERROR
    server = smtplib.SMTP_SSL(smtp_server, 465)
    msg = MIMEMultipart()
    msg['Subject'] = Header('%s Error Report' % model, 'utf-8' )
    msg['From'] = Header(from_addr, 'utf-8')
    msg['To'] = Header(to_addr[0], 'utf-8')
    msg.attach(MIMEText('{}'.format(error)))
    server.login(from_addr, password)
    server.sendmail(from_addr, to_addr, msg.as_string())
    print("Successfully sent error email")
    server.quit()


if __name__ == '__main__':
    report = Report(write=True, send=True)
    # obj.weak_report(curDateTime=pd.to_datetime('2021-08-16 00:00:00'))
    report.month_report(curDateTime=pd.to_datetime('2021-08-20 00:00:00'))
