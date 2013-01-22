#!/usr/bin/env python
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
Extension of the SCOOP bootstrap.__main__
"""
import argparse
import functools
import os
import runpy
import sys
import vsc.processcontrol
from distutils.version import LooseVersion
from scoop import futures
from scoop.bootstrap.__main__ import Bootstrap
from vsc.mympirun.scoop.worker_utils import set_scoop_env
from vsc.processcontrol.affinity import what_affinity
from vsc.processcontrol.priority import what_priority


class MyBootstrap(Bootstrap):
    def makeParser(self):
        super(MyBootstrap, self).makeParser()

        self.parser.add_argument('--processcontrol',
                                 help="Processcontrol mode",
                                 action='store',
                                 default=None
                                 )

        self.parser.add_argument('--nice',
                                 help="Set this nice level",
                                 action='store',
                                 default=0,
                                 type=int
                                 )

        self.parser.add_argument('--affinity',
                                 help="Affinity parameters",
                                 action='store',
                                 default=None
                                 )

    def parse(self):
        super(MyBootstrap, self).parse()

        # custom
        self.set_nice()
        self.set_affinity()
        self.set_environment()

    def set_nice(self):
        """Set the nice/priority level"""
        if self.args.nice is None:
            return

        control = what_priority(mode=self.args.processcontrol)
        if len(control) == 0:
            # do nothing?
            self.log.error("set_nice no prioritymode found for %s" % self.args.processcontrol)
            pass
        else:
            c = control[0]()
            c.set_priority(self.args.nice)

    def set_affinity(self):
        """Set the affinity"""
        if self.args.affinity is None:
            return

        affinityargs = self.args.affinity.split(':')
        algo = affinityargs.pop(0)
        control = what_affinity(mode=self.args.processcontrol,
                                algo=algo
                                )
        if len(control) == 0:
            # do nothing?
            self.log.error("set_affinity no affinitymode and algorithm found for %s and %s" % (self.args.processcontrol, algo))
            pass
        else:
            c = control[0]()
            c.algorithm(*affinityargs)

    def set_environment(self):
        """Set a number of worker environment variables"""
        set_scoop_env('worker_name', self.args.workerName)
        set_scoop_env('worker_origin', int(self.args.origin))

    def run(self):
        super(MyBootstrap, self).run(globs=globals())

if __name__ == "__main__":
    mb = MyBootstrap()
    mb.main()

