##
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
import os
from time import time
from vsc.utils.run import run_simple
from vsc.fancylogger import getLogger, setLogLevelDebug, logToFile, logToScreen, setLogLevelWarning, disableDefaultHandlers


NAME = 'simple_shell'
_DEBUG = True


def worker_run_simple(counter):
    """Execute the cmd
        to be called with
    """
    setLogLevelWarning()
    set_scoop_env('counter', counter)
    cmd_sanity = ["%s" % x for x in sys.argv[1:]]  ## ready to join
    ec, out = run_simple(' '.join(cmd_sanity))
    return  ec, out  ## return 1 item


SCOOP_ENVIRONMENT_PREFIX = 'SCOOP'
SCOOP_ENVIRONMENT_SEPARATOR = "_"

def set_scoop_env(name, value):
    envname = "".join([SCOOP_ENVIRONMENT_PREFIX, SCOOP_ENVIRONMENT_SEPARATOR, name.upper()])
    os.environ[envname] = "%s" % value  ## must be string


def main_run(main_func, *args, **kwargs):
    """Call this function to start"""
    _logm.debug("main_run: starting main_func %s with args %s kwargs %s" % (main_func, args, kwargs))
    res = None
    bt = time()
    try:
        res = main_func(*args, **kwargs)
    except:
        _logm.exception("main_run: main failed with main_func %s with args %s kwargs %s" % (main_func, args, kwargs))
    total_time = time() - bt

    ## write the output
    restxt = "%s" % res
    sys.stdout.write(restxt)
    sys.stdout.flush()

    _logm.debug("main_run: ending with res %s in total_time %s" % (res, total_time))

    return res

def main_extended_mapper(func, start, stop, wrap=True):
    """Main mapper to run the executable with mapping from start to stop (incl stop)"""
    #from scoop import futures  ## import here (initialises too many things ?)

    if wrap:
        wrapped_func = make_workerwrapper(func)
        _log.debug("main_extended_func: wrapped worker func %s in %s" % (func, wrapped_func))
    else:
        wrapped_func = func
        _log.debug("main_extended_func: no wrapping,  worker func %s" % (func))

    res = None
    try:
        res_generator = futures.map(wrapped_func, xrange(start, stop + 1))
        res = [x for x in res_generator]
    except:
        _log.exception("main_run: main failed with main_func %s with start %s stop %s" % (wrapped_func, start, stop))

    _log.debug("main_extended_func: res %s" % (res))
    return res


def main_mapper(func, start, stop, wrap=True):
    """Main mapper to run the executable with mapping from start to stop (incl stop)"""
    #from scoop import futures  ## import here (initialises too many things ?)

    if wrap:
        wrapped_func = make_workerwrapper(func)
        _log.debug("main_func: wrapped worker func %s in %s" % (func, wrapped_func))
    else:
        wrapped_func = func
        _log.debug("main_func: no wrapping,  worker func %s" % (func))

    res_generator = futures.map(wrapped_func, xrange(start, stop + 1))
    res = [x for x in res_generator]
    return res


def make_workerwrapper(workerfunc):
    """Make a wrapper to easily capture exceptions that occur on function evaluation on workers"""
    def _run(*args, **kwargs):
        """Intermediate step to help debugging"""
        try:
            return workerfunc(*args, **kwargs)
        except Exception, err:
            _log.exception("_run: workerfunc %s args %s kwargs %s err %s" % (workerfunc, args, kwargs, err))
            return err
    return _run

def make_workerwrapper_counter(workerfunc, usesysargv=True):
    """Make a simple workerfunction wrapper
        a counter is set in environment and function is wrapped and  called without arguments
    """
    def _run(counter):
        newfunc = make_workerwrapper(workerfunc)  # super is confused
        set_scoop_env('counter', counter)
        if usesysargv:
            cmd_sanity = ["%s" % x for x in sys.argv[1:]]  ## ready to join
            return newfunc(cmd_sanity)
        else:
            return newfunc()

    return _run

if __name__ == '__main__':
    from scoop import futures  # TODO always needed ?

    _log = getLogger(name=NAME)
    logfn = '/tmp/scoop_%s.log' % NAME
    ## TODO: touch and check ownership/permission
    logToFile(logfn, name=NAME)
    disableDefaultHandlers()



    if _DEBUG:
        setLogLevelDebug()



    ## disable stdout

    worker_func = worker_run_simple
    #worker_func = make_workerwrapper_counter(worker_run_simple)

    #main_run(main_mapper, worker_func, 30, 50)

    #print main_extended_mapper(worker_func, 30, 50)

    res = None
    start, stop = 30, 50
    try:
        res_generator = futures.map(worker_func, xrange(start, stop + 1))
        res = [x for x in res_generator]
    except:
        _log.exception("main_run: main failed with main_func %s with start %s stop %s" % (worker_func, start, stop))

    print res


