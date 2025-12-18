import logging, logging.handlers, os, sys



def setup_logger(app_name, log_format, log_level = logging.INFO, debug=False):
    log_dir = os.environ.get("jenkins_log_dir")
    if not log_dir:
        print(f"Unable to initialize the '{log_dir}'. Please check the environment the pipeline is running in.")
        sys.exit(4)
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)

    log_file = f"{log_dir}/{app_name}.log"

    formatter = logging.Formatter(log_format)

    if debug:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger

