# #
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
# #
"""
SCOOP support
    http://code.google.com/p/scoop/
    based on 0.6.0B code

This is not a MPI implementation at all.

Code is very lightweight.
"""
import itertools
import os
import socket
import subprocess  # TODO replace with run module
import sys
import time
from distutils.version import LooseVersion
from threading import Thread
from vsc.mympirun.mpi.mpi import MPI
from vsc.mympirun.exceptions import WrongPythonVersionExcpetion, InitImportException


from vsc.fancylogger import getLogger, setLogLevelDebug, logToFile
from vsc.utils.run import RunAsyncLoop
_logger = getLogger("MYSCOOP")

import signal

# # requires Python 2.6 at least (str.format)
if LooseVersion(".".join(["%s" % x for x in sys.version_info])) < LooseVersion('2.6'):
    _logger.raiseException("MYSCOOP / scoop requires python 2.6 or later", WrongPythonVersionExcpetion)


try:
    import scoop
except:
    _logger.raiseException("MYSCOOP requires the scoop module and scoop requires (amongst others) pyzmq",
                           InitImportException)
from scoop.__main__ import ScoopApp
from scoop.launch import Host
from scoop import utils

try:
    signal.signal(signal.SIGQUIT, utils.KeyboardInterruptHandler)
except AttributeError:
    # SIGQUIT doesn't exist on Windows
    signal.signal(signal.SIGTERM, utils.KeyboardInterruptHandler)


class MyHost(Host):
    BOOTSTRAP_MODULE = 'vsc.mympirun.scoop.__main__'

class MyScoopApp(ScoopApp):
    LAUNCH_HOST_CLASS = MyHost


class MYSCOOP(MPI):
    """Re-implement the launchScoop class from scoop.__main__"""
    SCOOP_APP = MyScoopApp

    SCOOP_WORKER_DIGITS = 5  # # 100k workers
    # # this module used to be "scoop.bootstrap.__main__"
    SCOOP_BOOTSTRAP_MODULE = 'vsc.mympirun.scoop.__main__'
    SCOOP_WORKER_MODULE_DEFAULT_NS = 'vsc.mympirun.scoop.worker'
    SCOOP_WORKER_MODULE_DEFAULT = 'simple_shell'

    _mpiscriptname_for = ['myscoop']

    RUNTIMEOPTION = {'options':{'tunnel':("Activate ssh tunnels to route toward the broker "
                                          "sockets over remote connections (may eliminate "
                                          "routing problems and activate encryption but "
                                          "slows down communications)", None, "store_true", False),
                                'broker':("The externally routable broker hostname / ip "
                                          "(defaults to the local hostname)", "str", "store", None),
                                'module':("Specifiy SCOOP worker module (to be imported or predefined in %s)" %
                                          SCOOP_WORKER_MODULE_DEFAULT_NS,
                                          "str", "store", SCOOP_WORKER_MODULE_DEFAULT),  # TODO provide list
                                },
                     'prefix':'scoop',
                     'description': ('SCOOP options', 'Advanced options specific for SCOOP'),
                     }
    def __init__(self, options, cmdargs, **kwargs):
        super(MYSCOOP, self).__init__(options, cmdargs, **kwargs)

        # # all SCOOP options are ready can be added on command line ? (add them to RUNTIMEOPTION)
        # # TODO : actually decide on wether they are options or not and
        # #   and change most of the code form self.scoop_X to self.options.scoop_X
        # #  (except for executable and args)

        allargs = self.cmdargs[:]
        exe = allargs.pop(0)

        self.scoop_size = getattr(self.options, 'scoop_size', None)
        self.scoop_hosts = getattr(self.options, 'scoop_hosts', None)
        self.scoop_python = getattr(self.options, 'scoop_python', sys.executable)
        self.scoop_pythonpath = getattr(self.options, 'scoop_pythonpath', [os.environ.get('PYTHONPATH', '')])

        self.scoop_executable = getattr(self.options, 'scoop_executable', exe)
        self.scoop_args = getattr(self.options, 'scoop_args', allargs)
        self.scoop_module = getattr(self.options, 'scoop_module', self.SCOOP_WORKER_MODULE_DEFAULT)

        self.scoop_nice = getattr(self.options, 'scoop_nice', 0)
        self.scoop_affinity = getattr(self.options, 'scoop_affinity', 'simplesinglecoreworker')
        self.scoop_path = getattr(self.options, 'scoop_path', os.getcwd())

        # # default broker is first of unique nodes ?
        self.scoop_broker = getattr(self.options, 'scoop_broker', None)
        self.scoop_brokerport = getattr(self.options, 'scoop_brokerport', None)

        self.scoop_infobroker = getattr(self.options, 'scoop_infobroker', self.scoop_broker)
        self.scoop_infoport = getattr(self.options, 'scoop_brokerport', None)

        self.scoop_origin = getattr(self.options, 'scoop_origin', False)
        self.scoop_debug = getattr(self.options, 'scoop_debug', self.options.debug)

        if self.scoop_debug:
            scoop_verbose = 2
        else:
            scoop_verbose = 1  # default loglevel is info
        self.scoop_verbose = getattr(self.options, 'scoop_verbose', scoop_verbose)

        self.scoop_tunnel = getattr(self.options, 'scoop_tunnel', False)

        self.scoop_profile = getattr(self.options, 'scoop_profile', True)

        self.scoop_remote = {}
        self.scoop_workers_free = None


    def main(self):
        """Main method"""
        self.prepare()

        self.scoop_prepare()
        self.scoop_make_executable()

        self.scoop_run()

        self.cleanup()

    def scoop_make_executable(self):
        """Create the proper scoop module to launch"""
        def _get_module(module_name):
            """Get the module basename
                returns None if failed
            """
            module_fn = None
            try:
                __import__(module_name)
            except:
                self.log.debug("_get_module: import module_name %s failed" % (module_name))
                return None

            try:
                module_fn = sys.modules[module_name].__file__.rsplit('.', 1)[0]
            except:
                self.log.raiseException("_get_module: import module_name %s succesful, can't locate file" %
                                        (module_name))

            self.log.debug("_get_module: module_name %s returned module_fn %s" % (module_name, module_fn))
            return module_fn

        if not self.scoop_executable.endswith('.py'):
            self.scoop_args = [self.scoop_executable] + self.scoop_args

            module_fn = _get_module(self.scoop_module)
            if module_fn is None:
                module_fn = _get_module('%s.%s' % (self.SCOOP_WORKER_MODULE_DEFAULT_NS, self.scoop_module))

                if module_fn is None:
                    self.log.raiseException("scoop_make_executable: failed to locate module %s (default NS %s)" %
                                            (self.scoop_module, self.SCOOP_WORKER_MODULE_DEFAULT_NS))

            # # some mode example runs are in vsc.mympirun.scoop
            self.scoop_executable = "%s.py" % module_fn
            self.log.debug("scoop_make_executable: from scoop_module %s executable %s args %s" % (
                            self.scoop_module, self.scoop_executable, self.scoop_args))

    def scoop_prepare(self):
        """Prepare the scoop parameters and commands"""
        # # self.mpinodes is the node list to use
        if self.scoop_broker is None:
            if self.mpdboot_localhost_interface is None:
                self.mpdboot_set_localhost_interface()
            self.scoop_broker = self.mpdboot_localhost_interface[0]

        if self.scoop_size is None:
            self.scoop_size = self.mpitotalppn * self.nruniquenodes
        if self.scoop_hosts is None:
            self.scoop_hosts = self.mpinodes

        if self.scoop_broker is None:
            # # default broker is first of unique nodes ?
            self.scoop_broker = self.uniquenodes[0]

        if self.scoop_infobroker is None:
            self.scoop_infobroker = self.scoop_broker


    def scoop_get_origin(self):
        # TODO remove
        """origin"""
        if self.scoop_workers_free == 1:
            self.log.debug('scoop_get_origin: set origin on')
            return "--origin"

    def scoop_get_debug(self):
        # TODO remove
        """debug"""
        if self.options.debug or self.scoop_debug:
            self.log.debug('scoop_get_debug: set debug on')
            return "--debug"

    def scoop_run(self):
        """Run the launcher"""

        # # previous scoop.__main__ main()


        scoop_app_args = [[(nodename, len(list(group))) for nodename, group in itertools.groupby(self.scoop_hosts)],
                          self.scoop_size,
                          self.scoop_verbose,
                          [self.scoop_python],
                          self.scoop_broker,
                          [self.scoop_executable],
                          self.scoop_args,
                          self.scoop_tunnel,
                          None,  # TODO args.log, deal with fancylogger later
                          self.scoop_path,
                          self.scoop_debug,
                          self.scoop_nice,
                          self.scoop_affinity,
                          "other",  # TODO check utils.getEnv(),
                          self.scoop_profile,
                          self.scoop_pythonpath[0]
                          ]
        self.log.debug("scoop_run: scoop_app class %s args %s" % (self.SCOOP_APP.__name__, scoop_app_args))

        scoop_app = self.SCOOP_APP(*scoop_app_args)
        try:
            root_task_ec = scoop_app.run()
            self.log.debug("scoop_run exited with exitcode %s" % root_task_ec)
        except Exception as e:
            self.log.exception('scoop_run: error while launching SCOOP subprocesses: {0}'.format(str(e)))
        finally:
            scoop_app.close()



