"""
    # @Time    : 2021/8/16 11:00
    # @Author  : Hong Yuanyang
    # @File    : ear_tang_and_death_model_online.py
    # @return  : 耳标状态和死亡判别模型
"""

import logging

# mongodb 生产环境详细配置
MONGO_HOST = "47.99.46.149"
MONGO_PORT = 20000
MONGO_DB = "animal"
MONGO_COLLECTIONS_TEMP_HISTORY = "temp_history"
MONGO_COLLECTIONS_ENV_TEMPERATURE_RECORD = "env_temperature_record"

# MySql 环境详细配置
MYSQL_HOST = "172.168.1.14"
MYSQL_PORT = 3306  # 生产环境 port 3999
MYSQL_USER = "hongyuanyang"
MYSQL_PASSWORD = "!tKST9*8iezHSRRzX6G*wPH*"

# 邮件配置信息
SMTP_SERVER = 'smtp.qq.com'
FROM_ADDRESS = '2327526320@qq.com'
PASS_WORD = 'jepqhkfnzpahecfi'  # 'tmggplmbscisecec'
TO_ADDRESS = ['2327526320@qq.com']  # '125-1x1y16kkv7@dingtalk.com','1093562028@qq.com','zzj029@dingtalk.com'
TO_ADDRESS_ERROR = ['2327526320@qq.com']
WRITE = True  # 写入报告状态
SEND = True  # 发送邮寄状态

# MySql表名配置
ANIMAL_MARK_TABLE = 'animal_mark_1'
HAVE_DEATH_TABLE = 'animal_death_info'
DEATH_MODEL_TABLE = 'animal_death_info_pre_1'  # 死亡模型人工预审表
EAR_TAG_MODEL_TABLE = 'device_alarm_1'  # 耳标状态模型表
EAR_TAG_UNDETERMINED_TABLE = 'device_alarm_undetermined_1'  # 耳标状态待定表
ANIMAL_REALTIME_TABLE = 'animal_realtime_info_1'  # 实时状态更新表
EAR_TAG_PARAM_TABLE = 'animal_ear_tag_param_hong_1'  # 耳标模型参数记录表
DEATH_PARAM_TABLE = 'animal_death_param_hong_1'  # 死亡模型参数记录表

RUNNING_MODE = 'VAL'  # 运行模式

DEFAULT_ENV_TEMPERATURE = 22  # 默认环境温度
DEFAULT_EAR_TEMPERATURE = 34  # 默认正常耳标温度
DELTA_TEMP_THRESHOLD = 5  # 平均温度差阈值
TEMP_DIFF_THRESHOLD = 4  # 温度差阈值
TEMP_MAX_THRESHOLD = 45   # 损坏判别温度最大阈值
TEMP_MIN_THRESHOLD = -20  # 损坏判别温度最小阈值
TAG_OFF_TEMP_THRESHOLD = 2  # 脱落温度比率
TAG_OFF_TEMP_RATE = 0.9  # 脱落温度比例
MAX_TEMP_CHANGE_RATE = 0.2  # 最大单位温度变化比率
MAX_TEMP_CHANGE_THRESHOLD = 3   # 最大单位温度变化阈值
MINIMUM_TEMP_CHANGE_THRESHOLD = 5  # 最低温度变化阈值
MAXIMUM_TEMP_CHANGE_THRESHOLD = 20  # 最高温度变化阈值
MINIMUM_EAR_TEMP = 30  # 最低正常耳标温度
SPORT_THRESHOLD = 15  # 运动量阈值
NOT_WEAR_SPORT_RATE_THRESHOLD = 0.9  # 未佩戴模型运动占比阈值
SPORT_RATE_THRESHOLD = 0.8  # 运动占比阈值
TEMP_DIFF_RATE_THRESHOLD = 0.8  # 温度差占比阈值
TAG_OFF_SPORT_RATE_THRESHOLD = 0.8  # 脱落运动占比阈值
TAG_OFF_SPORT_AMENDMENT_RATE_THRESHOLD = 0.8  # 脱落修正运动占比阈值

BACK_PERIOD = 24    # 总回溯的数据区间/h
EMPTY_DATA_PERIOD = 6   # 空数据判别区间
TAG_OFF_PERIOD = 1  # 脱落模型判别区间/h
# TAG_OFF_PRIED = 2   # 脱落模型比较间隔/h
TAG_OFF_AMENDMENT_PRIED = 8  # 脱落修正区间
TAG_OFF_BACK_PRIED = 6  # 脱落模型回溯区间
TAG_OFF_PRIED_TIMES = 2   # 脱落模型出现空数据回溯区间 = TAG_OFF_PRIED x TAG_OFF_PRIED_TIMES
DEATH_PERIOD = 2  # 死亡模型回溯区间/h
DEATH_COW_PERIOD = 4  # 牛死亡模型回溯区间/h
DEATH_COW_AMENDMENT_PRIED = 4  # 牛的死亡修正区间
DEATH_PIG_AMENDMENT_PRIED = 4  # 猪的死亡修正区间
DAY_TIME = 10   # 白天开始时间
NIGHT_TIME = 22  # 夜晚开始时间

TYPE_OFF_PIG_LIST = [12, 468118228439138304, 481235871098470400, 489424434898141184, 494524276976648192,
                     503655401628106752, 494524629172355072, 503655401602940928]
TYPE_OFF_COW_LIST = [101, 102, 625016322975072256, 495324503417552896, 495326683977482240, 495588200689958912,
                     495588200715124736, 495588200740290560, 605113810780426240, 605113810813980672, 607970402957787136,
                     625016322949906432, 625016322975072256, 625016323000238080, 625016323025403904, 625016323046375424]

LOG_DIR = 'logs'  # 日志目录
LOG_PATH = 'ai_ear_tang_and_death_model.log'  # 日志路径
LOG_NAME = 'AI_MODEL'  # 日志名称
HANDLER_LEVEL = logging.WARNING  # 写入级别
CONSOLE_LEVEL = logging.ERROR  # 输出级别

# RUN:单次运行
# VAL:回溯历史数据运行
START_TIME = '2021-01-01 22:30:00'
END_TIME = '2021-01-02 23:59:59'
LABEL_NUMBER = None
TIME_PRIED = 30  # 运行间隔/min
DEATH_PRIED = 30  # 死亡模型运行间隔
# REAL:实时运行
# TIMING:定时运行
FIRM_ID = None
# [590633020835233792, 536581099812290560, 600004590590492672, 614823169068367872]
# 590633020835233792: '长坪',
# 536581099812290560: '余庆县长坪生猪养殖场',
# 600004590590492672: "傲农余庆刘江猪场",
# 614823169068367872: "益万家养殖农民专业合作社"

DEATH_MODEL_FILE = 'param/death_model_012.h5'
DEATH_SCALER_FILE = 'param/death_model_012_scaler.m'
DEATH_JUDGE_RATE = 0.9
DEATH_ADJUST_RATE = 0.5
SEQ_LEN = 120
N_CHANNELS = 6

TAG_OFF_FILE = 'param/ear_tag_model_022.h5'
TAG_OFF_SCALER_FILE = 'param/ear_tag_model_022_scaler.m'
TAG_OFF_JUDGE_RATE = 0.9
TAG_OFF_ADJUST_RATE = 0.5
TAG_OFF_SEQ_LEN = 120


