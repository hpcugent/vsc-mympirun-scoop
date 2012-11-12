##
# Copyright 2012 Ghent University
# Copyright 2012 Stijn De Weirdt
#
# This file is part of VSC-tools,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/VSC-tools
#
# VSC-tools is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
#
# VSC-tools is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VSC-tools. If not, see <http://www.gnu.org/licenses/>.
##
"""
SCOOP run of command and args in repeated environment
    provide environment variables so apps can benefit
"""
import sys
from vsc.utils.run import run_simple
from vsc.mympirun.scoop.worker_utils import set_scoop_env, parse_worker_args, make_worker_log
from scoop import futures

NAME = 'simple_shell'
_DEBUG = True

def worker_run_simple(counter):
    """Execute the cmd
        to be called with
    """
    cmd_sanity = ["%s" % x for x in parse_worker_args()]  ## ready to join
    set_scoop_env('counter', counter)
    ec, out = run_simple(' '.join(cmd_sanity), disable_log=True)

    return  ec, out  ## return 1 item

if __name__ == '__main__':
    _log = make_worker_log(NAME, debug=_DEBUG)

    worker_func = worker_run_simple

    res = None
    start, stop, step = parse_worker_args(False)
    try:
        _log.debug("main_run: going to start map")
        res_generator = futures.map(worker_func, xrange(start, stop, step))
        _log.debug("main_run: finished map")
        res = [x for x in res_generator]
        _log.debug("main_run: finished res from generator")
    except:
        _log.exception("main_run: main failed with main_func %s with start %s stop %s" % (worker_func, start, stop))

    print res


