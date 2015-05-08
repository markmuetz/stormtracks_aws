import logging

def setup_logging(name, filename, mode='a'):
    log = logging.getLogger(name)

    if name == 'st_worker_status':
        formatter = logging.Formatter('%(message)s')
    else:
        formatter = logging.Formatter('%(asctime)s %(name)-16s %(levelname)-8s %(message)s')
    fileHandler = logging.FileHandler(filename, mode=mode)
    fileHandler.setFormatter(formatter)

    streamFormatter = logging.Formatter('%(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(streamFormatter)

    log.setLevel(logging.DEBUG)
    log.addHandler(fileHandler)
    log.addHandler(streamHandler)  

    return log
