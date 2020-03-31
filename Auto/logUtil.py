import logging
from logging import handlers

def getLog():
    # 初始化logger
    logger = logging.getLogger()
    # 设置日志记录级别
    logger.setLevel(logging.INFO)
    # fmt设置日志输出格式,datefmt设置 asctime 的时间格式
    formatter = logging.Formatter(fmt='[%(asctime)s]%(levelname)s:%(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    # 配置日志输出到控制台
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)
    # 配置日志输出到文件,在固定的时间内记录日志文件
    file_time_rotating = handlers.TimedRotatingFileHandler("app_time.log", when="D", interval=1, backupCount=7,encoding='utf-8')
    file_time_rotating.setLevel(logging.INFO)
    file_time_rotating.setFormatter(formatter)
    logger.addHandler(file_time_rotating)

    return logger

log = getLog()