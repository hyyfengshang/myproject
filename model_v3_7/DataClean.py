import pymysql
from config import *
from log import logger
from datetime import timedelta


class DataClean(object):
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

    def clean_data(self, curDateTime, delete_interval=30):
        delete_time = curDateTime - timedelta(days=delete_interval)
        delete_death_param = "DELETE FROM {} WHERE update_time < %s;".format(DEATH_PARAM_TABLE)
        self.cursor.execute(delete_death_param, delete_time)
        self.conn.commit()
        delete_ear_tag_param = "DELETE FROM {} WHERE update_time < %s;".format(EAR_TAG_PARAM_TABLE)
        self.cursor.execute(delete_ear_tag_param, delete_time)
        self.conn.commit()

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    import pandas as pd

    curdatatime = pd.to_datetime('2021-08-05 00:00:00')
    obj = DataClean()
    obj.clean_data(curdatatime, delete_interval=1)
    obj.close()
