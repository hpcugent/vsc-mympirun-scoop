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
Small test to print some execution details and statistics
"""
import os
import sys
import time
from vsc.mympirun.scoop.worker_utils import get_scoop_env, fix_freeorigin
from scoop import futures

try:
    import psutil
    HAS_PSUTIL = True
except:
    HAS_PSUTIL = False

NAME = 'sanity'
_DEBUG = True

def sanity(counter):
    s_t = time.time()
    worker = get_scoop_env('worker_name')
    origin = get_scoop_env('worker_origin')
    freeorigin = get_scoop_env('worker_freeorigin')
    delta = time.time() - s_t
    if HAS_PSUTIL:
        affinity = psutil.Process(os.getpid()).get_cpu_affinity()
    else:
        affinity = None
    return counter, worker, origin, delta, affinity, freeorigin

if __name__ == '__main__':
    nr_batches = 1000
    try:
        nr_batches = int(sys.argv[1])
    except:
        pass

    fix_freeorigin()

    s_t = time.time()
    res_generator = futures.map(sanity, xrange(nr_batches))
    res = [x for x in res_generator]
    delta = time.time() - s_t

    ## avg runtime
    global_total_time = sum([x[3] for x in res])
    global_avg_time = 1.0 * global_total_time / nr_batches
    print "GLOBAL nr_batches %d avg_time %fs total_duration %ss" % (nr_batches, global_avg_time, delta)

    workers = dict([(x, []) for x in set([y[1] for y in res])])
    for y in res:
        workers[y[1]].append((y[3], y[4], "%s/%s" % (y[2], y[5])))

    ## TODO use scipy statistics. but you get the point
    ## TODO remove origin worker from stats
    print "avg %d, min %d, max %d number of batches per worker" % (nr_batches / len(workers),
                                                                   min([len(x) for x in workers.values()]),
                                                                   max([len(x) for x in workers.values()]),
                                                                   )
    for w in workers:
        print "  Worker %s nr_batches %s affinity %s (origin %s)" % (w,
                                                         len(workers[w]),
                                                         workers[w][0][1],
                                                         workers[w][0][2],
                                                         )
