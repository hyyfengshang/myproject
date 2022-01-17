"""
    默认配置文件，不可修改
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

LOG_DIR = 'logs'  # 日志目录
LOG_PATH = 'ai_ear_tag_and_death_model.log'  # 日志路径
LOG_NAME = 'AI_MODEL'  # 日志名称
HANDLER_LEVEL = logging.WARNING  # 写入级别
CONSOLE_LEVEL = logging.ERROR  # 输出级别