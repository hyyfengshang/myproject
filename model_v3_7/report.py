import pymysql
from config import *
import pandas as pd
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart


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
        self.write = write
        self.send = send

    def get_tag_off_data(self, curDateTime, back_time):
        end_dt = curDateTime.strftime('%Y%m%d%H%M%S')
        start_dt = (curDateTime - timedelta(days=back_time)).strftime('%Y%m%d%H%M%S')
        query_tag_off_data = "SELECT mark_id,label_number,update_time FROM {} WHERE `status` = 2 AND label_number LIKE '10000010%'" \
                             "AND update_time >= {} AND update_time < {}".format(
            EAR_TAG_MODEL_TABLE, start_dt, end_dt)
        tag_off_data = pd.read_sql(query_tag_off_data, con=self.conn)
        data = self.get_farm_data(tag_off_data)
        return data

    def get_destroy_data(self, curDateTime, back_time):
        end_dt = curDateTime.strftime('%Y%m%d%H%M%S')
        start_dt = (curDateTime - timedelta(days=back_time)).strftime('%Y%m%d%H%M%S')
        query_destroy_data = "SELECT mark_id,label_number,update_time FROM {} WHERE `status` = -1 AND label_number LIKE '10000010%'" \
                             "AND update_time >= {} AND update_time < {}".format(
            EAR_TAG_MODEL_TABLE, start_dt, end_dt)
        destroy_data = pd.read_sql(query_destroy_data, con=self.conn)
        data = self.get_farm_data(destroy_data)
        return data

    def get_death_data(self, curDateTime, back_time):
        end_dt = curDateTime.strftime('%Y%m%d%H%M%S')
        start_dt = (curDateTime - timedelta(days=back_time)).strftime('%Y%m%d%H%M%S')
        query_death_data = "SELECT mark_id,label_number,death_time from {} WHERE death_type=1 AND label_number LIKE '10000010%'" \
                           "AND death_time >= {} AND death_time < {}".format(
            DEATH_MODEL_TABLE, start_dt, end_dt)
        death_data = pd.read_sql(query_death_data, con=self.conn)
        data = self.get_farm_data(death_data)
        return data

    def get_farm_data(self, backtrackdata=None):
        out_put_data = []
        for data in backtrackdata.iterrows():
            mark_id = data[1].values[0]
            label_number = data[1].values[1]
            update_time = pd.to_datetime(data[1].values[2]).strftime('%Y-%m-%d %H:%M:%S')
            query_farm_data = "SELECT column_id,farm_id,type_conf_id FROM animal_mark WHERE id ={}".format(mark_id)
            farm_data = pd.read_sql(query_farm_data, con=self.conn)
            if len(farm_data) == 0:
                continue
            column_id = farm_data.iloc[0]['column_id']
            farm_id = farm_data.iloc[0]['farm_id']
            type_conf_id = farm_data.iloc[0]['type_conf_id']
            query_farm_name = "SELECT name FROM sys_farm WHERE id ={}".format(farm_id)
            farm_name = pd.read_sql(query_farm_name, con=self.conn)
            if len(farm_name) == 0:
                continue
            farm_name = farm_name.iloc[0][0]
            query_column_data = "SELECT parent_id,place_name FROM animal_farm_place WHERE id ={}".format(column_id)
            column_data = pd.read_sql(query_column_data, con=self.conn)
            if len(column_data) == 0:
                continue
            parent_id = column_data.iloc[0]['parent_id']
            column_name = column_data.iloc[0]['place_name']
            house_name = None
            if int(parent_id) != 0:
                query_house_name = "SELECT place_name FROM animal_farm_place WHERE id ={}".format(parent_id)
                house_name = pd.read_sql(query_house_name, con=self.conn)
                if len(house_name) == 0:
                    continue
                house_name = house_name.iloc[0][0]
            query_type_name = "SELECT `name` FROM conf_type_info WHERE id = {}".format(type_conf_id)
            type_name = pd.read_sql(query_type_name, con=self.conn)
            if len(type_name) == 0:
                continue
            type_name = type_name.iloc[0][0]
            single_data = [label_number, update_time, farm_name, house_name, column_name, type_name]
            out_put_data.append(single_data)
        return out_put_data

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
    def __sendReport(mold, flieXpath, curDateTime, back_time):
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

    def __dataGet(self, curDateTime, back_time):
        death_data = self.get_death_data(curDateTime, back_time)
        tag_off_data = self.get_tag_off_data(curDateTime, back_time)
        damage_data = self.get_destroy_data(curDateTime, back_time)
        data = [death_data, tag_off_data, damage_data]
        return data

    def __report(self, curDateTime, back_time, saveXpath, mold, write=False, send_email=False):
        if not os.path.exists(saveXpath):
            os.makedirs(saveXpath)
        data_list = self.__dataGet(curDateTime, back_time)
        if write:
            fileXpath_list = self.__WriteReport(curDateTime, saveXpath, mold, data_list)
            if send_email:
                self.__sendReport(mold, fileXpath_list, curDateTime, back_time)

    def day_report(self, curDateTime):
        self.__report(curDateTime=curDateTime, back_time=1, saveXpath='./report/day_report',
                      mold='Daily', write=self.write, send_email=self.send)

    def weak_report(self, curDateTime):
        self.__report(curDateTime=curDateTime, back_time=7, saveXpath='./report/weak_report',
                      mold='Weakly', write=self.write, send_email=self.send)

    def month_report(self, curDateTime):
        self.__report(curDateTime=curDateTime, back_time=30, saveXpath='./report/month_report',
                      mold='Monthly', write=self.write, send_email=self.send)

    def all_report(self, curDateTime):
        self.day_report(curDateTime)
        self.weak_report(curDateTime)
        self.month_report(curDateTime)

    def close(self):
        self.cursor.close()
        self.conn.close()


def send_error_report(model, error):
    smtp_server = SMTP_SERVER
    from_addr = FROM_ADDRESS
    password = PASS_WORD
    to_addr = TO_ADDRESS_ERROR
    server = smtplib.SMTP_SSL(smtp_server, 465)
    msg = MIMEMultipart()
    msg['Subject'] = Header('%s Error Report' % model, 'utf-8')
    msg['From'] = Header(from_addr, 'utf-8')
    msg['To'] = Header(to_addr[0], 'utf-8')
    msg.attach(MIMEText('{}'.format(error)))
    server.login(from_addr, password)
    server.sendmail(from_addr, to_addr, msg.as_string())
    print("Successfully sent error email")
    server.quit()


if __name__ == '__main__':
    obj = Report(send=True)
    obj.day_report(pd.to_datetime("2021-10-18 00:00:00"))
    obj.close()
