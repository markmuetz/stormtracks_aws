import os

# For use on ubuntu EC2 instance.
SETTINGS_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.expandvars('$HOME/stormtracks_data/data')
OUTPUT_DIR = os.path.expandvars('$HOME/stormtracks_data/output')
LOGGING_DIR = os.path.expandvars('$HOME/stormtracks_data/logs')
FIGURE_OUTPUT_DIR = os.path.expandvars('$HOME/stormtracks_figures/')

C20_FULL_DATA_DIR = os.path.join(DATA_DIR, 'c20_full')
C20_GRIB_DATA_DIR = os.path.join(DATA_DIR, 'c20_grib')
C20_MEAN_DATA_DIR = os.path.join(DATA_DIR, 'c20_mean')
IBTRACS_DATA_DIR = os.path.join(DATA_DIR, 'ibtracs')
