#
# Copyright 2012-2013 Ghent University
# Copyright 2012-2013 Stijn De Weirdt
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
#
"""
SCOOP support
    http://code.google.com/p/scoop/
    based on 0.6.0  code
"""
import itertools
import os
import sys
from collections import namedtuple
from distutils.version import LooseVersion
from vsc.fancylogger import getLogger
from vsc.mympirun.mpi.mpi import MPI
from vsc.mympirun.exceptions import WrongPythonVersionExcpetion, InitImportException

_logger = getLogger("MYSCOOP")

# requires Python 2.6 at least (str.format)
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

class MyHost(Host):
    BOOTSTRAP_MODULE = 'vsc.mympirun.scoop.bootstrap'
    LAUNCHING_ARGUMENTS = namedtuple(Host.LAUNCHING_ARGUMENTS.__name__,
                                     list(Host.LAUNCHING_ARGUMENTS._fields) +
                                     ['processcontrol', 'affinity',
                                      'variables']
                                     )

    def _WorkerCommand_environment(self, worker):
        c = super(MyHost, self)._WorkerCommand_environment(worker)

        set_variables = self._WorkerCommand_environment_set_variables(worker.variables)
        # TODO do we need the module load when we pass most variables?

        return set_variables + c

    def _WorkerCommand_environment_set_variables(self, variables):
        # TODO port to env when super(MyHost, self)._WorkerCommand_environment(worker) does this
        shell_template = "export {name}='{value}'"

        cmd = []
        for name, value in [(x, os.environ.get(x)) for x in variables if x in os.environ]:
            txt = shell_template.format(name=name, value=value)
            cmd.extend([txt, '&&'])

        return cmd

    def _WorkerCommand_environment_load_modules(self):
        # TODO what is needed here? VSC-tools mympirun-scoop too.
        load_modules = ['SCOOP']
        mod_load = []
        if load_modules is not None:
            mod_load.extend(['module', 'load'])
            for mod_to_load in load_modules:
                # check something first?
                mod_load.append(mod_to_load)
            mod_load.append('&&')


        return mod_load

    def _WorkerCommand_bootstrap(self, worker):
        # nice will be passed as argument
        newworker = worker._replace(nice=None)  # worker is namedtuple instance
        c = super(MyHost, self)._WorkerCommand_bootstrap(newworker)
        return c

    def _WorkerCommand_options(self, worker, workerId):
        c = super(MyHost, self)._WorkerCommand_options(worker, workerId)
        if worker.processcontrol is not None:
            self.log.debug("WorkerCommand_options processcontrol %s" % worker.processcontrol)
            c.extend(['--processcontrol', worker.processcontrol])
            if worker.nice is not None:
                self.log.debug("WorkerCommand_options nice %s" % worker.nice)
                c.extend(['--nice', str(worker.nice)])
            if worker.affinity is not None:
                self.log.debug("WorkerCommand_options affinity %s" % worker.affinity)
                c.extend(['--affinity', '{algorithm}:{total_workers_host}:{worker_idx_host}'.format(**worker.affinity)])
        else:
            if worker.nice is not None:
                self.log.error("nice is set, but no processcontrol")
            if worker.affinity is not None:
                self.log.error("affinity is set, but no processcontrol")
        return c


class MyScoopApp(ScoopApp):
    LAUNCH_HOST_CLASS = MyHost

    def __init__(self, *args):
        args = list(args)  # args here is tuple, need to chaneg it (ie remove affintiy arg)
        # remove custom options
        self.variables_to_pass = args.pop()
        self.affinity = args.pop()
        self.processcontrol = args.pop()
        super(MyScoopApp, self).__init__(*args)

    def _addWorker_args(self, workerinfo):
        args, kwargs = super(MyScoopApp, self)._addWorker_args(workerinfo)
        # tuple with lots of info
        kwargs['processcontrol'] = self.processcontrol
        affinity = workerinfo.copy()
        affinity['algorithm'] = self.affinity
        kwargs['affinity'] = affinity
        kwargs['variables'] = self.variables_to_pass
        return args, kwargs


class MYSCOOP(MPI):
    """Re-implement the launchScoop class from scoop.__main__"""
    SCOOP_APP = MyScoopApp

    SCOOP_WORKER_DIGITS = 5  # 100k workers
    # this module used to be "scoop.bootstrap.__main__"
    SCOOP_WORKER_MODULE_DEFAULT_NS = 'vsc.mympirun.scoop.worker'
    SCOOP_WORKER_MODULE_DEFAULT = 'simple_shell'

    PASS_VARIABLES_CLASS_PREFIX = ['SCOOP']  # used for anything?

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
                                'profile':("Turn on SCOOP profiling", None, "store_true", False),
                                },
                     'prefix':'scoop',
                     'description': ('SCOOP options', 'Advanced options specific for SCOOP'),
                     }
    def __init__(self, options, cmdargs, **kwargs):
        super(MYSCOOP, self).__init__(options, cmdargs, **kwargs)

        # all SCOOP options are ready can be added on command line ? (add them to RUNTIMEOPTION)
        # TODO : actually decide on wether they are options or not and
        #   and change most of the code form self.scoop_X to self.options.scoop_X
        #  (except for executable and args)

        allargs = self.cmdargs[:]
        exe = allargs.pop(0)

        self.scoop_size = getattr(self.options, 'scoop_size', None)
        self.scoop_hosts = getattr(self.options, 'scoop_hosts', None)
        self.scoop_python = getattr(self.options, 'scoop_python', sys.executable)
        self.scoop_pythonpath = getattr(self.options, 'scoop_pythonpath', [os.environ.get('PYTHONPATH', '')])

        self.scoop_executable = getattr(self.options, 'scoop_executable', exe)
        self.scoop_args = getattr(self.options, 'scoop_args', allargs)
        self.scoop_module = getattr(self.options, 'scoop_module', self.SCOOP_WORKER_MODULE_DEFAULT)

        self.scoop_processcontrol = getattr(self.options, 'scoop_processcontrol', 'VSC')
        self.scoop_nice = getattr(self.options, 'scoop_nice', 0)
        self.scoop_affinity = getattr(self.options, 'scoop_affinity', 'basiccore')  # the algorithm

        self.scoop_path = getattr(self.options, 'scoop_path', os.getcwd())

        # default broker is first of unique nodes ?
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

        self.scoop_profile = getattr(self.options, 'scoop_profile', False)

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

            # some mode example runs are in vsc.mympirun.scoop
            self.scoop_executable = "%s.py" % module_fn
            self.log.debug("scoop_make_executable: from scoop_module %s executable %s args %s" % (
                            self.scoop_module, self.scoop_executable, self.scoop_args))

    def scoop_prepare(self):
        """Prepare the scoop parameters and commands"""
        # self.mpinodes is the node list to use
        if self.scoop_broker is None:
            if self.mpdboot_localhost_interface is None:
                self.mpdboot_set_localhost_interface()
            self.scoop_broker = self.mpdboot_localhost_interface[0]

        if self.scoop_size is None:
            self.scoop_size = self.mpitotalppn * self.nruniquenodes
        if self.scoop_hosts is None:
            self.scoop_hosts = self.mpinodes

        if self.scoop_broker is None:
            # default broker is first of unique nodes ?
            self.scoop_broker = self.uniquenodes[0]

        if self.scoop_infobroker is None:
            self.scoop_infobroker = self.scoop_broker

    def scoop_run(self):
        """Run the launcher"""
        vars_to_pass = self.get_pass_variables()
        # add uniquenodes that are localhost
        localhosts = self.get_localhosts()
        utils.localHostnames.extend([hn for hn, ip in localhosts if not hn in utils.localHostnames])

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
                          "other",  # TODO check utils.getEnv(),
                          self.scoop_profile,
                          self.scoop_pythonpath[0],
                          # custom
                          self.scoop_processcontrol,
                          self.scoop_affinity,
                          vars_to_pass,
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



