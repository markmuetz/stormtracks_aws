#!/usr/bin/env python
from subprocess import call, check_output
from time import sleep
import datetime as dt


def get_percent_mem_used(prog='st_worker'):
    cmd1 = 'pgrep {0}'.format(prog)
    pidno = check_output(cmd1, shell=True)
    cmd2 = "top -bn1 -p {0}".format(pidno.strip())
    top_out = check_output(cmd2, shell=True)
    top_lines = top_out.split('\n')
    mem_usage_percent = float(top_lines[7].split()[9])
    return mem_usage_percent


def get_sys_used_free_mem():
    cmd = "free -m"
    free_out = check_output(cmd, shell=True)
    free_lines = free_out.split('\n')
    used, free = map(int, free_lines[2].split()[2:4])
    return used, free


def main(filename='/home/ubuntu/stormtracks_data/logs/vital_stats.log'):
    with open(filename, 'a') as f:
        f.write('date,st_worker_mem_usage(%),used(Mb),free(Mb)\n')
        while True:
            date = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S.%f")
            mem_usage_percent = get_percent_mem_used()
            used, free = get_sys_used_free_mem()
            f.write('{0},{1},{2},{3}\n'.format(date, mem_usage_percent, used, free))
            f.flush()
            sleep(10)


if __name__ == '__main__':
    main()
