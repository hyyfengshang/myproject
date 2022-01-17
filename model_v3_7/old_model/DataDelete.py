import pymysql
from old_code.configuration import *
from log import logger
import pandas as pd
import numpy as np
from datetime import timedelta


class DataDelete(object):
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

    def delete_product_data(self):
        delete_death_table = "DELETE FROM {} ;".format(DEATH_MODEL_TABLE)
        self.cursor.execute(delete_death_table)
        self.conn.commit()
        delete_ear_tag_table = "DELETE FROM {} ;".format(EAR_TAG_MODEL_TABLE)
        self.cursor.execute(delete_ear_tag_table)
        self.conn.commit()

    def delete_param_data(self, last_date_time=None):
        query_earliest_datetime = "SELECT MIN(update_time) FROM {}".format(EAR_TAG_PARAM_TABLE)
        query_longest_datetime = "SELECT MAX(update_time) FROM {}".format(EAR_TAG_PARAM_TABLE)
        earliest_datetime = np.array(pd.read_sql(query_earliest_datetime, con=self.conn))[0][0]
        longest_datetime = np.array(pd.read_sql(query_longest_datetime, con=self.conn))[0][0]
        earliest_date_time = pd.to_datetime(earliest_datetime)
        longest_date_time = pd.to_datetime(longest_datetime)
        if last_date_time is not None:
            longest_date_time = pd.to_datetime(last_date_time)
        while earliest_date_time < longest_date_time:
            delete_time = earliest_date_time + timedelta(days=1)
            delete_death_param = "DELETE FROM {} WHERE update_time < %s;".format(DEATH_PARAM_TABLE)
            self.cursor.execute(delete_death_param, delete_time)
            self.conn.commit()
            delete_ear_tag_param = "DELETE FROM {} WHERE update_time < %s;".format(EAR_TAG_PARAM_TABLE)
            self.cursor.execute(delete_ear_tag_param, delete_time)
            self.conn.commit()
            earliest_date_time = earliest_date_time + timedelta(days=1)

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    obj = DataDelete()
    obj.delete_product_data()
    obj.delete_param_data()
    obj.close()
    print('Delete Successful')
