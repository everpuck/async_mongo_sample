# encoding=utf-8
import os
import json
import logging.config

cur_path = os.path.dirname(os.path.realpath(__file__))
DEFAULT_CONFIG_FILE = os.path.join(cur_path, 'mylog.config')

# log section
if os.name == 'nt':
    DEFAULT_LOG_PATH = 'C:\\dalog'
else:
    DEFAULT_LOG_PATH = '/tmp/darklog'


def createDir(path):
    if not os.path.isdir(path):
        os.mkdir(path)

'''
    特点：
        1、出现异常时，可以打印异常堆栈
        2、loggger使用完后，可以不用释放handler，在次申请同样logger，打印日志不会重复
'''


def setup_logger(
    logger_name='root',
    logger_path=None,
    is_show_logname=False,
    default_config=DEFAULT_CONFIG_FILE,
        env_key='LOG_CFG'):
    '''
    Function:
        apply logger
    Desciption:
        Firstly load logger config module file
        Secondly config owner logger
        Finally apply logger
    Params:
        logger_name: the name of applied logger
        logger_path: the path of logger file, if not exist, create it
        is_show_logname: if is_show_logname is True, show logger_name into output log
        default_config: logger config module
        env_key: the name of logger config file in the environment
    '''
    def config_logger(config, logger_file):
        handlers = []
        for i, key in enumerate(config["handlers"]):
            handlers.append(key)
            if is_show_logname:
                # config output format
                config["handlers"][key]["formatter"] = "complex"
            # config output file name
            if key == "info_file_handler":
                config["handlers"][key]["filename"] = logger_file + ".info.log"
            elif key == "error_file_handler":  #如果不想输出error文件， 可以注销
                config["handlers"][key]["filename"] = logger_file + ".error.log"

        # if do not input error file, pop it
        config['handlers'].pop('error_file_handler', None)
        handlers.remove('error_file_handler')

        # config apply logger
        config["loggers"][logger_name] = { \
                "level": "DEBUG", \
                "handlers": handlers, \
                "propagate": "no"}

    # create log directory
    logger_path = logger_path if logger_path else DEFAULT_LOG_PATH
    createDir(logger_path)
    logger_file = os.path.join(logger_path, logger_name)

    config_file = os.path.join(os.path.dirname(__file__), default_config)
    value = os.getenv(env_key, None)
    if value:
        config_file = value
    if os.path.exists(config_file):
        # load logger config module file
        with open(config_file, 'rt') as f:
            config = json.load(f)
        if config:
            # config owner logger
            config_logger(config, logger_file)
            logging.config.dictConfig(config)
            return logging.getLogger(logger_name)
    else:
        raise Exception("logger config file {0} is not exist.".format(config_file))

if "__main__" == __name__:
    logger = setup_logger(logger_name='test', logger_path=None, is_show_logname=False)
    logger.info("xxxx info")
    logger.error("xxxx error")
    try:
        raise Exception("ee")
    except Exception as e:
        logger.warning("except", exc_info=True)
    logger.debug("xxxx debug")

    logging = setup_logger(logger_name='test', is_show_logname=True)
    logging.info("xxxx info")
    logging.error("xxxx error")
