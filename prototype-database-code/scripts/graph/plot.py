import matplotlib
matplotlib.use('Agg')

matplotlib.rcParams['figure.figsize'] = 5, 1.6
matplotlib.rcParams['lines.linewidth'] = 2

from pylab import *
from os import listdir
from sys import argv
import re

if len(argv) == 1:
    rootdir = "../output-wan"
else:
    rootdir = argv[1]

class RunResult:
    def __init__(self, ops, time, lats, avg):
        self.ops = ops
        self.time = time
        self.lats = lats
        self.avg = avg
        self.merges = 0
        self.thrus = []
        self.lat_all = []

    def get_thru(self):
        return self.ops/(self.time/1000.)*self.merges

    def get_pctile_latency(self, pctile):
        target = self.ops*pctile

        total = 0
        for bucketno in range(0, len(self.lats)):
            total += self.lats[bucketno]
            if total >= target:
                return bucketno

    def get_average_latency(self):
        return self.avg/1000.

    def mergeResult(self, other):
        self.merges += 1
        self.time += other.time
        self.ops += other.ops
        self.thrus.append(other.ops/other.time*1000.)
        self.lat_all.append(other.get_pctile_latency(.99))#avg/1000.)

        totalops = float(self.ops+other.ops)
        self.avg = self.avg*(self.ops/totalops)+other.avg*(other.ops/totalops)

        if len(self.lats) == 0:
            self.lats = other.lats
            return True
            
        if len(self.lats) != len(other.lats):
            print "LATENCY MISMATCH:", len(self.lats), len(other.lats)
            return False

        for bucketno in range(0, len(self.lats)):
            self.lats[bucketno] += other.lats[bucketno]

        return True

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def processYCSB(d):
    lats = []

    ops = None
    avglat = None
    time = None

    for line in open(d+"/run_out.log"):
        line = line.split()
        if len(line) == 0:
            continue
        if line[0].find("OVERALL") != -1:
            if line[1].find("RunTime") != -1:
                time = float(line[2])
        if line[0].find("TRANSACTION") != -1:
            if line[1].find("AverageLatency") != -1:
                avglat = float(line[2])
            elif line[1].find("Operations") != -1:
                ops = int(line[2])
            elif is_number(line[1][:-1]) or is_number(line[1][1:-1]):
                if is_number(line[2]):
                    lats.append(int(line[2]))
                else:
                    print d, line

    if ops is None or time is None or len(lats) == 0 or avglat is None:
        print d
        print ops, time, len(lats), avglat
        return None

    return RunResult(ops, time, lats, avglat)

def processConfig(d):
    combinedResult = RunResult(0, 0, [], 0)
    for cd in listdir(d):
        if cd[0] == "C":
            result = processYCSB(d+"/"+cd)
            if result is None:
                print "PROCESSING FAILED ON", d
                return None
            if not combinedResult.mergeResult(result):
                print "MERGING FAILED ON", d
                return None

    #combinedResult.thrus.sort()
    #print d, combinedResult.thrus
    print d, combinedResult.lat_all
    return combinedResult

results = {}

txnlens = []

for config in listdir(rootdir):
    '''
    p = processConfig(rootdir+"/"+config)
    print config, p.get_thru(), p.get_average_latency(), p.get_pctile_latency(.999)
    continue
    '''

    match = re.match("[A-Z_]+-([0-9]+)-([A-Z_]+-[A-Z_]+-)?THREADS([0-9]+)-RPROP([\.0-9]+)-NS([0-9]+)-IT([0-9]+)$", config)
    if match:
        txnlength = int(match.group(1))
        nthreads = int(match.group(3))
        rprop = float(match.group(4))
        ns = int(match.group(5))
        it  = int(match.group(6))
    else:
        # Legacy -- remove eventually
        match = re.match("[A-Z_]+-([0-9]+)-([A-Z_]+-[A-Z_]+-)?THREADS([0-9]+)-IT([0-9]+)$", config)
        if match:
            txnlength = int(match.group(1))
            nthreads = int(match.group(3))
            rprop = 0.5
            ns = '?'
            it  = int(match.group(4))
        else:
            print 'Error: Could not pattern match configuration: %s' % config
            continue

    if int(txnlength) not in txnlens:
        txnlens.append(int(txnlength))

    configsplit=config.split("-THREADS")
    configstr = configsplit[0].replace("-%d-" % txnlength, "")
    configstr = configstr.replace("-%d" % txnlength, "")

    r = processConfig(rootdir+"/"+config)
    if r is None:
        print "SKIPPING", config
        '''
        if nthreads in results[configstr]:
            del results[configstr][nthreads]
        '''
        continue
    r.threads = nthreads
    r.txnlength = int(txnlength)
    r.rprop = rprop
    r.ns = ns

    if configstr not in results:
        results[configstr] = {}
    if nthreads not in results[configstr]:
        results[configstr][nthreads] = {}
    if rprop not in results[configstr][nthreads]:
        results[configstr][nthreads][rprop] = {}
    if ns not in results[configstr][nthreads][rprop]:
        results[configstr][nthreads][rprop][ns] = []

    results[configstr][nthreads][rprop][ns].append(r)

txnlens.sort()

PCT = 0
AVG = 1

fmtdict = {
"CONSTANT_TRANSACTIONREAD_COMMITTED-NO_ATOMICITY" : ["blue", '^-'],
"QUORUM_EVENTUAL" : ["black", 'v-'],
"MASTERED_EVENTUAL" : ["teal", 's-'],
"CONSTANT_TRANSACTIONREAD_COMMITTED-CLIENT" : ["green", 'o-'],
"EVENTUAL" : ["red", 'x-']}

threadresultthru = {}
threadresultlat = {}

for latplot in [AVG]:
    for txnlen in txnlens:
        for cs in results.keys():
            pairs = []

            if cs not in threadresultthru:
                threadresultthru[cs] = []
                threadresultlat[cs] = []
            
            for nthreads in results[cs].keys():
                for rprop in results[cs][nthreads]:
                    for ns in results[cs][nthreads][rprop]:

                        tresults = results[cs][nthreads][rprop][ns]
                        avgthru = median([r.get_thru() for r in tresults])
                        avglat = median([r.get_average_latency() for r in tresults])
                        #todo, fix

                        threadresultthru[cs].append([nthreads*6, avgthru/1000.0])
                        threadresultlat[cs].append([nthreads*6, avglat])

                        r = results[cs][nthreads][rprop][ns][0]

                        if str(r.txnlength) != str(txnlen):
                            continue

                        print cs, txnlen, "THREADS", r.threads, "RPROP", r.rprop,
                        if ns != '?':
                            print "NS", r.ns,
                        print "THRU", avgthru, "LATS", avglat, len(results[cs][nthreads]), [r.get_thru() for r in tresults]# r.get_average_latency(), r.get_pctile_latency(.999)

                        lat = avglat

                        pairs.append([avgthru, lat])

            pairs.sort(key=lambda x: x[1])

            curthru = -1
            filterpairs = []
            for pair in pairs:
                if pair[0] > .95*curthru:
                    curthru = pair[0]
                    pair[0] = pair[0]/1000.0
                    filterpairs.append(pair)

            pairs = filterpairs

            #l = cs.replace("CONSTANT_TRANSACTION", "")
            
            #l = l.replace("_EVENTUAL", "")
            
            plot([p[0] for p in pairs], [p[1] for p in pairs], fmtdict[cs][1], color=fmtdict[cs][0], markeredgecolor=fmtdict[cs][0], markerfacecolor='None')

        #legend(loc="upper right")
        #title("Transactions of length "+str(txnlen))
        gca().set_yscale('log')
        #xlabel("Throughput (Txns/s)")
        #ylabel(("99.9th Percentile" if latplot == PCT else "Average")+" Latency (ms)")

        ax = gca()
        ax.yaxis.grid(True, which='major')
        ax.yaxis.grid(False, which='minor')

        legstr = "PCTILE" if latplot == PCT else "AVG"

        savefig(str(txnlen)+legstr+"-plot.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)
        clf()

print 'Prepare for trouble!'
for cs in threadresultthru:
    print 'And make it double!'
    results = threadresultthru[cs]
    results.sort(key=lambda x: x[0])
    print cs,results
    
    plot([p[0] for p in results], [p[1] for p in results],  fmtdict[cs][1], color=fmtdict[cs][0], markeredgecolor=fmtdict[cs][0], markerfacecolor='None')

ylabel("Throughput (1000 Txns/s)")
xlabel("Number of Clients")
xlim(xmax=600)
savefig("threads-thru.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)
print 'ok...'
clf()

for cs in threadresultlat:
    results = threadresultlat[cs]
    results.sort(key=lambda x: x[0])
    
    plot([p[0] for p in results], [p[1] for p in results],  fmtdict[cs][1], color=fmtdict[cs][0], markeredgecolor=fmtdict[cs][0], markerfacecolor='None')

xlim(xmax=600)
ax = gca()
gca().set_yscale('log')
ax.yaxis.grid(True, which='major')
ax.yaxis.grid(False, which='minor')
ylabel("Average Latency (ms)")
savefig("threads-lats.pdf", transparent=True, bbox_inches='tight', pad_inches=.1)
clf()
