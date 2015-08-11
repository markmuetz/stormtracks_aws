#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
import os
from glob import glob
import datetime as dt

import numpy as np
import pylab as plt

from commandify import commandify, command, main_command

@main_command
def main():
    pass

def get_hosts():
    os.chdir('logs/remote')

    hosts = sorted(glob('*'))
    os.chdir('../../')
    return hosts


@command
def list_hosts():
    for host in get_hosts():
        print(host)


def check_vital_stat_line(line):
    parts = line.strip().split(',')
    if parts[0] == 'date':
        return False
    return True

def read_vital_stat_line(line, parse_df=False):
    parts = line.strip().split(',')
    date = dt.datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S.%f')
    percentage_mem = float(parts[1])
    used_ram = int(parts[2])
    free_ram = int(parts[3])
    if parse_df:
        df = float(parts[4])
        return date, percentage_mem, used_ram, free_ram, df
    else:
        return date, percentage_mem, used_ram, free_ram


@command
def parse_all_vital_stats(col=1, parse_df=False):
    run_times = []
    for host in get_hosts():
        vals = parse_vital_stats(host, col, parse_df, plot=False)
        plt.plot(vals[:, 0], vals[:, col])
        run_times.append((vals[-1, 0] - vals[0, 0]).total_seconds() / 3600.)

    plt.figure()
    plt.plot(range(len(run_times)), sorted(run_times))
    plt.show()


@command
def parse_vital_stats(host, col=1, parse_df=False, plot=True):
    os.chdir('logs/remote/{0}/logs'.format(host))
    lines = open('vital_stats.log', 'r').readlines()

    if parse_df:
        def read_line(line):
            return read_vital_stat_line(line, True)
    else:
        read_line = read_vital_stat_line

    vals = np.array(map(read_line, filter(check_vital_stat_line, lines)))
    os.chdir('../../../../')
    if plot:
        plt.plot(vals[:, 0], vals[:, col])
        plt.show()
    else:
        return vals


@command
def parse_all_analysis(plot='deltas'):
    for host in get_hosts():
        dates, deltas = parse_analysis(host, plot=False)
        if plot == 'deltas':
            plt.plot([d.total_seconds() for d in deltas])
        elif plot == 'dates':
            plt.plot(dates)

    if plot == 'deltas':
        plt.ylim(0, 20)
    plt.show()


@command
def parse_analysis(host, plot='deltas'):
    os.chdir('logs/remote/{0}/logs'.format(host))
    lines = open('analysis.log', 'r').readlines()
    dates = [dt.datetime.strptime(line[:23] + '00', '%Y-%m-%d %H:%M:%S,%f') for line in lines]
    deltas = []
    for i in range(len(dates) - 1):
        deltas.append(dates[i + 1] - dates[i])
    os.chdir('../../../../')
    if plot == 'deltas':
        plt.plot([d.total_seconds() for d in deltas])
        plt.ylim(0, 20)
        plt.show()
    elif plot == 'dates':
        plt.plot(dates)
        plt.show()
    return dates, deltas


if __name__ == '__main__':
    commandify(suppress_warnings=['default_true'],
               use_argcomplete=True)
