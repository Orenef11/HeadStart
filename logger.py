import logging
from os import makedirs, path, getcwd

SPACE = '\n'
INFO_MODE = True
MODE = 'a'
LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}


def setting_up_logger(file_logging_level, console_logging_level, file_path):
    file_logging_level = str(file_logging_level).lower()
    console_logging_level = str(console_logging_level).lower()
    if file_logging_level not in LEVELS or console_logging_level not in LEVELS:
        print("Error log logging: The values to be inserted are: ", LEVELS.keys())
        exit()

    create_log_file = False
    folder_path = path.split(file_path)[0]
    if not path.isdir(folder_path) and folder_path != '':
        makedirs(folder_path)
        create_log_file = True
    # set up logging to file - see previous section for more details
    format_logging = "%(asctime)s\t[%(filename)s %(funcName)s %(lineno)d]  %(msecs)d %(name)s [%(levelname)-5.5s] " \
                     "%(message)s"
    logging.basicConfig(level=LEVELS[file_logging_level], format=format_logging, datefmt='%d/%m/%Y %H:%M:%S',
                        filename=file_path, filemode=MODE)
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(LEVELS[console_logging_level])
    # set a format which is simpler for console use
    formatter = logging.Formatter(format_logging)
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

    if create_log_file:
        logging.debug(SPACE + "Create '{0}' folder in {1} path".format(folder_path, getcwd()))
        logging.info(SPACE + "Create '{0}' folder in {1} path".format(folder_path, getcwd()))


def change_logger_file(old_logger_file_path, new_logger_file_path, mode='debug'):
    mode = mode.lower()
    if mode not in LEVELS:
        print("Error log logging: The values to be inserted are: ", LEVELS.keys())
        exit()

    new_logger_folder_path = path.split(new_logger_file_path)[0]
    if not path.isdir(new_logger_folder_path):
        makedirs(new_logger_folder_path)
        logging.debug(SPACE + "Create '{0}' folder in {1} path".format(new_logger_folder_path, getcwd()))
        logging.info(SPACE + "Create '{0}' folder in {1} path".format(new_logger_folder_path, getcwd()))

    log = logging.getLogger()
    logger_change_flag = False
    old_log_handler = ''
    for log_handler in log.handlers:
        if isinstance(log_handler, logging.FileHandler) and \
                (old_logger_file_path == "" or str(log_handler.baseFilename).endswith(old_logger_file_path)):
            old_logger_file_path = log_handler.baseFilename
            old_log_handler = log_handler
            logger_change_flag = True
            break

    if not logger_change_flag:
        logging.debug(SPACE + "{} file that not exist".format(old_logger_file_path))

    old_log_handler.stream.close()
    log.removeHandler(old_log_handler)
    file_handler = logging.FileHandler(new_logger_file_path, 'a')
    formatter = logging.Formatter("%(asctime)s\t[%(filename)s %(funcName)s %(lineno)d]  %(msecs)d %(name)s "
                                  "[%(levelname)-5.5s] %(message)s")
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
