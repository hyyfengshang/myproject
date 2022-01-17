"""
   日志文件
"""

import os
from config import *


def logger_config(_log_path, logging_name, handler_level=logging.ERROR, console_level=logging.ERROR):
    """
    配置log
    :param _log_path:
    :param console_level: 写入文件级别
    :param handler_level: 输出级别
    :param log_path: 输出log路径
    :param logging_name: 记录中name，可随意
    :return:
    """
    """
    logger是日志对象，handler是流处理器，console是控制台输出（没有console也可以，将不会在控制台输出，会在日志文件中输出）
    """

    # 获取logger对象,取名
    _logger = logging.getLogger(logging_name)
    # 输出DEBUG及以上级别的信息，针对所有输出的第一层过滤
    _logger.setLevel(level=logging.DEBUG)
    # 获取文件日志句柄并设置日志级别，第二层过滤
    handler = logging.FileHandler(log_path, encoding='UTF-8')
    handler.setLevel(handler_level)
    # 生成并设置文件日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # console相当于控制台输出，handler文件输出。获取流句柄并设置日志级别，第二层过滤
    console = logging.StreamHandler()
    console.setLevel(console_level)
    # 为logger对象添加句柄
    _logger.addHandler(handler)
    _logger.addHandler(console)
    return _logger


log_dir = LOG_DIR
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_path = os.path.join(log_dir, LOG_PATH)

logger = logger_config(_log_path=log_path, logging_name=LOG_NAME, handler_level=HANDLER_LEVEL, console_level=CONSOLE_LEVEL )

