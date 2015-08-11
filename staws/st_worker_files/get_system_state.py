#!/usr/bin/env python
"""Gets useful system information about the current system"""
from __future__ import print_function

import os
import subprocess


OUTPUT_DIR = '../logs/'
CMDS = (
    ('command', 'path', 'is_strip', 'venv'),
    ('uname -a', None, False, None),
    ('cat /etc/lsb-release',None , False, None),
    ('dpkg -l|grep ^ii|awk \'{print $2 "\t" $3}\'', None, False, None),
    # ('pip freeze', None, False, '$HOME/Projects/stormtracks/st_env'),
    ('git rev-parse HEAD', '$HOME/Projects/stormtracks', True, None),
    ('git rev-parse HEAD', '$HOME/Projects/stormtracks_aws', True, None),
    )


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    with open(os.path.join(OUTPUT_DIR, 'system_state.txt'), 'w') as cmds_txt:
        for cmd, path, is_strip, venv in CMDS[1:]:
            if venv:
                # Activate virtualenv.
                cmd = '. {0} && {1}'.format(os.path.expandvars(venv), cmd)

            cmds_txt.write('=' * 80 + '\n')
            cmds_txt.write(cmd + '\n')
            cmds_txt.write('=' * 80 + '\n')
            print(cmd)

            if path:
                # Change to correct dir.
                cwd = os.getcwd()
                os.chdir(os.path.expandvars(path))

            result = subprocess.check_output(cmd, shell=True)
            if is_strip:
                result = result.strip()

            if path:
                os.chdir(cwd)

            cmds_txt.write(result)
            cmds_txt.write('\n')


if __name__ == '__main__':
    main()
