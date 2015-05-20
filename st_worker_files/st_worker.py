#!/home/ubuntu/Projects/stormtracks/st_env/bin/python
"""
Will be run from st_worker ubuntu EC2 instance.

Imports functions from stormtracks and stormtracks_aws and calls those functions to perform analysis
on years defined by st_worker_settings.YEARS. Will download all NetCDF4 files for given years then
perform tracking/matching analysis, then collect fields based on tracks.  Finally zips and sends all
output to S3, then tidies up after itself (deletes NetCDF4 files) before starting on next year. All
actions are logged to st_worker_status.log, which allows for (very simple) remote monitoring.
"""
# So I can access modules defined in parent dir.
import sys
sys.path.append('/home/ubuntu/Projects/stormtracks_aws')
import os
import multiprocessing as mp

from stormtracks.load_settings import settings
from stormtracks import download, analysis
from stormtracks.results import StormtracksResultsManager

from st_worker_settings import YEARS

from st_utils import setup_logging
from aws_helpers import upload_large_file

# So as paths to e.g. aws_credentials in upload_large_file work.
os.chdir('/home/ubuntu/Projects/stormtracks_aws')

# N.B. uses absolute path.
logging_filename = os.path.join(settings.LOGGING_DIR, 'st_worker_status.log')
log = setup_logging(name='st_worker_status', filename=logging_filename, mode='w')


def download_year_data(year):
    download.download_full_c20(year)


def logging_callback(msg):
    log.info(msg)


def analyse_year(year):
    sa = analysis.StormtracksAnalysis(year, profiling=True)
    sa.logging_callback = logging_callback
    config = sa.analysis_config_options[5]
    sa.run_full_analysis(config, 56)


def cross_ensemble_analyse_year(year):
    sa = analysis.StormtracksAnalysis(year, profiling=True)
    sa.logging_callback = logging_callback
    sa.run_cross_ensemble_analysis()


def compress_year_output(year):
    srm = StormtracksResultsManager('aws_tracking_analysis')
    compressed_filename = srm.compress_year(year, delete=True)
    return compressed_filename


def upload_year_s3(compressed_filename):
    upload_large_file(compressed_filename)


def delete_year_data(year):
    download.delete_full_c20(year)


def run_for_year(year):
    log.info('downloading year data {0}'.format(year))
    download_year_data(year)
    log.info('cross ensemble analysing year {0}'.format(year))
    # analyse_year(year)
    cross_ensemble_analyse_year(year)
    log.info('compressing year output {0}'.format(year))
    compressed_filename = compress_year_output(year)
    log.info('uploading year to s3 {0}'.format(year))
    upload_year_s3(compressed_filename)
    log.info('deleting year data {0}'.format(year))
    delete_year_data(year)


def main():
    for year in YEARS:
        try:
            proc = mp.Process(name='run_for_year', target=run_for_year,
                              kwargs={'year': year})
            log.info('Executing fabric commands')
            proc.start()
            proc.join()
        except Exception as e:
            log.error(e)
            log.error('Error with year'.format(year))
            raise e

    log.info('analysed years {0}-{1}'.format(YEARS[0], YEARS[-1]))


if __name__ == '__main__':
    main()
