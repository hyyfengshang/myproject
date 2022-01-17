import pymysql
from config import *
from param import *
from log import logger
from datetime import timedelta
import pandas as pd
import numpy as np
from tqdm import tqdm


class DataCheck(object):
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

    def data_check(self):
        query_ear_tag = "SELECT mark_id FROM {} WHERE deal_flag = 0 ".format(EAR_TAG_MODEL_TABLE)
        markIdData = np.array(pd.read_sql(query_ear_tag, con=self.conn))
        markIdList = [str(i[0]) for i in markIdData]
        for markId in tqdm(markIdList):
            query_status = "SELECT status FROM {} WHERE deal_flag = 0 and mark_id = {}".format(EAR_TAG_MODEL_TABLE,
                                                                                               markId)
            status = np.array(pd.read_sql(query_status, con=self.conn))[0][0]
            update_animal_realtime = "UPDATE {} SET tag_on=%s WHERE mark_id=%s ".format(ANIMAL_REALTIME_TABLE)

            self.cursor.execute(update_animal_realtime, (status, markId))
            self.conn.commit()

    def close(self):
        """
        关闭mysql
        """
        self.cursor.close()
        self.conn.close()


if __name__ == '__main__':
    obj = DataCheck()
    obj.data_check()
    obj.close()
