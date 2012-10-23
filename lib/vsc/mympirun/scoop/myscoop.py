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
SCOOP support
    http://code.google.com/p/scoop/
    based on 0.5.3 code

This is not a MPI implementation at all.

Code is very lightweight.
"""
import time
import sys
import os
import subprocess  # TODO replace with run module
from distutils.version import LooseVersion
from threading import Thread
from vsc.mympirun.mpi.mpi import MPI
from vsc.mympirun.exceptions import WrongPythonVersionExcpetion, InitImportException

from vsc.fancylogger import getLogger, setLogLevelDebug, logToFile
from vsc.utils.run import RunAsyncLoop
_logger = getLogger("MYSCOOP")

try:
    import scoop
except:
    _logger.raiseException("MYSCOOP requires the scoop module and scoop requires (amongst others) pyzmq",
                           InitImportException)

## requires Python 2.6 at least (str.format)
if LooseVersion(".".join(["%s" % x for x in sys.version_info])) < LooseVersion('2.6'):
    _logger.raiseException("MYSCOOP / scoop requires python 2.6 or later", WrongPythonVersionExcpetion)

class MYSCOOP(MPI):
    """Re-implement the launchScoop class from scoop.__main__"""
    SCOOP_WORKER_DIGITS = 5 ## 100k workers
    ## this module used to be "scoop.bootstrap.__main__"
    SCOOP_BOOTSTRAP_MODULE = 'vsc.mympirun.scoop.__main__'
    SCOOP_WORKER_MODULE = 'run_simple_shell'

    _mpiscriptname_for = ['myscoop']

    RUNTIMEOPTION = {'options':{'tunnel':("Activate ssh tunnels to route toward the broker "
                                          "sockets over remote connections (may eliminate "
                                          "routing problems and activate encryption but "
                                          "slows down communications)", None, "store_true", False),
                                'broker':("The externally routable broker hostname / ip "
                                          "(defaults to the local hostname)", "str", "store", None),
                                'module':("Use one of mympirun prepared SCOOP worker modules",
                                          "str", "store", SCOOP_WORKER_MODULE), # TODO provide list
                                },
                     'prefix':'scoop',
                     'description': ('SCOOP options', 'Advanced options specific for SCOOP'),
                     }
    def __init__(self, options, cmdargs, **kwargs):
        super(MYSCOOP, self).__init__(options, cmdargs, **kwargs)

        ## all SCOOP options are ready can be added on command line ? (add them to RUNTIMEOPTION)
        ## TODO : actually decide on wheter they are otions or not and
        ##   and change most of the code form self.scoop_X to self.options.scoop_X
        ##  (except for executable and args)
        self.scoop_size = getattr(self.options, 'scoop_size', None)
        self.scoop_hosts = getattr(self.options, 'scoop_hosts', None)
        self.scoop_python = getattr(self.options, 'scoop_python', sys.executable)

        allargs = self.cmdargs[:]
        exe = allargs.pop(0)
        self.scoop_executable = getattr(self.options, 'scoop_executable', exe)
        self.scoop_args = getattr(self.options, 'scoop_args', allargs)
        self.scoop_module = getattr(self.options, 'scoop_module', self.SCOOP_WORKER_MODULE)

        self.scoop_nice = getattr(self.options, 'scoop_nice', 0)
        self.scoop_affinity = getattr(self.options, 'scoop_affinity', None)
        self.scoop_path = getattr(self.options, 'scoop_path', os.getcwd())

        ## default broker is first of unique nodes ?
        self.scoop_broker = getattr(self.options, 'scoop_broker', None)
        self.scoop_brokerport = getattr(self.options, 'scoop_brokerport', None)

        self.scoop_infobroker = getattr(self.options, 'scoop_infobroker', self.scoop_broker)
        self.scoop_infoport = getattr(self.options, 'scoop_brokerport', None)

        self.scoop_origin = getattr(self.options, 'scoop_origin', False)
        self.scoop_debug = getattr(self.options, 'scoop_debug', self.options.debug)

        self.scoop_tunnel = getattr(self.options, 'scoop_tunnel', False)

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
        if not self.scoop_executable.endswith('.py'):
            self.scoop_args = [self.scoop_executable] + self.scoop_args


            module_name = 'vsc.mympirun.scoop.%s' % self.scoop_module
            try:
                __import__(module_name)
            except:
                self.log.raiseException("scoop_make_executable: failed to import %s" % module_name)

            try:
                module_fn = sys.modules[module_name].__file__.rsplit('.', 1)[0]
            except:
                self.log.raiseException(("scoop_make_executable: failed to locate module name %s in sys.modules "
                                         "(only names with scoop shown) %s") % (module_name,
                                        [x for x in sys.modules.keys() if 'scoop' in x]))

            ## some mode example runs are in vsc.mympirun.scoop
            self.scoop_executable = "%s.py" % module_fn
            self.log.debug("scoop_make_executable: from scoop_module %s executable %s args %s" % (
                            self.scoop_module, self.scoop_executable, self.scoop_args))

    def scoop_prepare(self):
        """Prepare the scoop parameters and commands"""
        ## self.mpinodes is the node list to use
        if self.scoop_broker is None:
            if self.mpdboot_localhost_interface is None:
                self.mpdboot_set_localhost_interface()
            self.scoop_broker = self.mpdboot_localhost_interface[0]

        if self.scoop_size is None:
            self.scoop_size = self.mpitotalppn * self.nruniquenodes
        if self.scoop_hosts is None:
            self.scoop_hosts = self.mpinodes

        if self.scoop_broker is None:
            ## default broker is first of unique nodes ?
            self.scoop_broker = self.uniquenodes[0]

        if self.scoop_infobroker is None:
            self.scoop_infobroker = self.scoop_broker

    def scoop_get_origin(self):
        """origin"""
        if self.scoop_workers_free == 1:
            self.log.debug('scoop_get_origin: set origin on')
            return "--origin"

    def scoop_get_debug(self):
        """debug"""
        if self.options.debug or self.scoop_debug:
            self.log.debug('scoop_get_debug: set debug on')
            return "--debug"

    def scoop_launch_foreign(self, w_id, affinity=None):
        """Create the foreign launch command
            similar to __main__.launchForeign
                assumes nodes can ssh into themself
            w_id is the workerid
        """
        if affinity is None:
            cmd_affinity = []
        else:
            cmd_affinity = ["--affinity", affinity]
        c = [self.scoop_python, '-u',
             "-m ", self.SCOOP_BOOTSTRAP_MODULE,
             "--workerName", "worker{0:0{width}}".format(w_id, width=self.SCOOP_WORKER_DIGITS),
             "--brokerName", "broker",
             "--brokerAddress", "tcp://{brokerHostname}:{brokerPort}".format(
                                        brokerHostname=self.scoop_broker,
                                        brokerPort=self.scoop_brokerport),
             "--metaAddress", "tcp://{infobrokerHostname}:{infoPort}".format(
                                        infobrokerHostname=self.scoop_infobroker,
                                        infoPort=self.scoop_infoport),
             "--size", str(self.scoop_size),
             "--startfrom", self.scoop_path,
             "--nice", self.scoop_nice,
             self.scoop_get_origin(),
             self.scoop_get_debug(),
             ] + cmd_affinity + [self.scoop_executable] + self.scoop_args
        self.log.debug("scoop_launch_foreign: command c %s" % c)
        return ["%s" % x for x in c if (x is not None) and (len("%s" % x) > 0) ]


    def scoop_start_broker(self):
        """Starts a broker on random unoccupied port(s)"""
        from scoop.broker import Broker  # import here to avoid issues with bootstrap TODO move bootstrap
        if self.scoop_broker in self.uniquenodes:
            self.log.debug("scoop_start_broker: broker %s in current nodeset, starting locally (debug %s)" %
                           (self.scoop_broker, self.scoop_debug))
            self.local_broker = Broker(debug=self.scoop_debug)
            self.scoop_brokerport, self.scoop_infoport = self.local_broker.getPorts()
            self.local_broker_process = Thread(target=self.local_broker.run)
            self.local_broker_process.daemon = True
            self.local_broker_process.start()
        else:
            ## try to start it remotely ?
            ## TODO: see if we can join an existing broker
            ##  (better yet, lets assume it is running and try to guess the ports)
            self.log.raiseException("scoop_start_broker: remote code not implemented")

    def scoop_get_affinity(self, w_id, u_id):
        """Determine the affinity of the scoop wroker
            w_id is the total workerid
            u_id is the index in the uniquehosts list
        """
        return u_id  # TODO: assumes 1 core per proc. what with hybrid etc etc

    def scoop_launch(self):
        # Launching the local broker, repeat until it works
        self.log.debug("scoop_run: initialising local broker.")
        self.scoop_start_broker()
        self.log.debug("scoop_run: local broker launched on brokerport {0}, infoport {1}"
                      ".".format(self.scoop_brokerport, self.scoop_infoport))

        # Launch the workers in mpitotalppn batches on each unique node
        if self.scoop_workers_free is None:
            self.scoop_workers_free = len(self.mpinodes)

        shell = None
        w_id = -1
        for host in self.uniquenodes:
            command = []
            for n in range(min(self.scoop_workers_free, self.mpitotalppn)):
                w_id += 1
                affinity = self.scoop_get_affinity(n, w_id)
                command.append(self.scoop_launch_foreign(w_id, affinity=affinity))
                self.scoop_workers_free -= 1

            # Launch every unique remote hosts at the same time
            if len(command) != 0:
                ssh_command = ['ssh', '-x', '-n', '-oStrictHostKeyChecking=no']
                if self.scoop_tunnel:
                    self.log.debug("run: adding ssh tunnels for broker and info port ")
                    ssh_command += ['-R {0}:127.0.0.1:{0}'.format(self.scoop_brokerport),
                                    '-R {0}:127.0.0.1:{0}'.format(self.scoop_infoport)
                                    ]
                print_bash_pgid = 'ps -o pgid= -p \$BASHPID'  # print bash group id to track it for kill
                ## join all commands as background process
                all_foreign_cmd = " ".join([" ".join(cmd + ['&']) for cmd in command])
                bash_cmd = " ".join([print_bash_pgid, '&&', all_foreign_cmd])

                full_cmd = ssh_command + [host, '"%s"' % bash_cmd]
                self.log.debug("scoop_run: going to start subprocess %s" % (" ".join(full_cmd)))
                shell = RunAsyncLoop(" ".join(full_cmd))
                shell._run_pre()
                self.scoop_remote[shell] = [host]
            if self.scoop_workers_free == 0:
                break

        self.log.debug("scoop_run: started on %s remotes, free workers %s" % (len(self.scoop_remote), self.scoop_workers_free))

        # Get group id from remote connections
        for remote in self.scoop_remote.keys():
            gid = remote._process.stdout.readline().strip()
            self.scoop_remote[remote].append(gid)
        self.log.debug("scoop_run: found remotes and pgid %s" % self.scoop_remote.values())

        # Wait for the root program
        # shell is last one, containing the origin
        if shell is None:
            self.log.raiseException("scoop_run: nothing started?")

        self.log.debug("scoop_run: rootprocess output")
        shell._wait_for_process()
        ec, out = shell._run_post()
        self.log.debug("scoop_run: rootprocess ended ec %s out %s" % (ec, out))

        return out

    def scoop_close(self):
        # Ensure everything is cleaned up on exit
        self.log.debug('scoop_close: destroying remote elements...')
        self.local_broker_process

        for data in self.scoop_remote.values():
            if len(data) > 1:
                host, pid = data
                ssh_command = ['ssh', '-x', '-n', '-oStrictHostKeyChecking=no', host]
                kill_cmd = "kill -9 -%s &>/dev/null" % pid  # kill -<level> -n : all processes in process group n are signaled.

                self.log.debug("scoop_close: host %s kill %s" % (host, kill_cmd))
                subprocess.Popen(ssh_command + ["bash", "-c", "'%s'" % kill_cmd]).wait()
            else:
                self.log.error('scoop_close: zombie process left')

        self.log.info('scoop_close: finished destroying spawned subprocesses.')


    def scoop_run(self):
        """Run the launcher"""

        ## previous scoop.__main__ main()
        res = None
        try:
            res = self.scoop_launch()
        except:
            self.log.exception("scoop_run: failure in scoop_launch")
        finally:
            self.scoop_close()

        ## write to stdout
        if res is not None:
            sys.stdout.write(res)
            sys.stdout.flush()


