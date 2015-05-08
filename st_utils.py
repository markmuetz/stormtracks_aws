import logging

def setup_logging(name, filename):
    log = logging.getLogger(name)

    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    fileHandler = logging.FileHandler(filename, mode='a')
    fileHandler.setFormatter(formatter)

    streamFormatter = logging.Formatter('%(message)s')
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(streamFormatter)

    log.setLevel(logging.DEBUG)
    log.addHandler(fileHandler)
    log.addHandler(streamHandler)  

    return log
