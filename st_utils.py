"""
Utilities for all stormtracks_aws files.
"""
import logging


def setup_logging(name, filename, mode='a', use_console=True):
    log = logging.getLogger(name)

    if name == 'st_worker_status':
        formatter = logging.Formatter('%(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s %(name)-16s %(levelname)-8s %(message)s')

    if filename is not None:
        fileHandler = logging.FileHandler(filename, mode=mode)
        fileHandler.setFormatter(formatter)
        log.setLevel(logging.DEBUG)
        log.addHandler(fileHandler)

    if use_console:
        streamFormatter = logging.Formatter('%(message)s')
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(streamFormatter)
        log.addHandler(streamHandler)

    return log
