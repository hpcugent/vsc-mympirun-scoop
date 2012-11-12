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
A collection of functions and constants to use within worker modules
"""
import os
import stat
import sys
from vsc.fancylogger import getLogger, setLogLevelDebug, logToFile, disableDefaultHandlers

SCOOP_ENVIRONMENT_PREFIX = 'SCOOP'
SCOOP_ENVIRONMENT_SEPARATOR = "_"

def make_worker_log(name, debug=False, logfn_name=None, disable_defaulthandlers=False):
    """Make a basic log object"""
    if logfn_name is None:
        logfn_name = name
    logfn = '/tmp/scoop_%s.log' % logfn_name

    if debug:
        setLogLevelDebug()

    logToFile(logfn, name=name)
    os.chmod(logfn, stat.S_IRUSR | stat.S_IWUSR)

    if disable_defaulthandlers:
        disableDefaultHandlers()

    _log = getLogger(name=name)

    return _log

def _get_scoop_env_name(name):
    """Geneate the SCOOP environment name"""
    envname = "".join([SCOOP_ENVIRONMENT_PREFIX, SCOOP_ENVIRONMENT_SEPARATOR, name.upper()])
    return envname

def set_scoop_env(name, value):
    """Set environment variables specific for SCOOP"""
    envname = _get_scoop_env_name(name)
    os.environ[envname] = "%s" % value  ## must be string

def get_scoop_env(name):
    """Get environment variables specific for SCOOP"""
    envname = _get_scoop_env_name(name)
    return os.environ.get(envname, None)

def parse_worker_args(executable=True):
    """Parse the arguments
        check if first arg matches [start:]stop[:step]
    """
    offset = 1

    start = 0
    stop = 10
    step = 1

    arg1 = sys.argv[1]
    try:
        arg1 = arg1.split(':')
        if len(arg1) == 1:
            stop = int(arg1[0])
        elif len(arg1) == 2:
            start = int(arg1[0])
            stop = int(arg1[1])
        elif len(arg1) == 3:
            start = int(arg1[0])
            stop = int(arg1[1])
            step = int(arg1[2])
        offset += 1
    except:
        pass

    if executable:
        return sys.argv[offset:]
    else:
        return start, stop, step

