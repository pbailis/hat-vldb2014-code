
from sys import argv


graphite_server="ec2-54-242-170-176.compute-1.amazonaws.com"

if len(argv) > 2:
    graphite_server = argv[1]

import urllib2

def fetchone(query):

    response = urllib2.urlopen("http://%s/render?format=raw&target=%s" % (graphite_server, query))
    html = response.read().replace("None,", "").replace(",None", "")

    html = html.split(',')
    retstr = html[len(html)-1].strip('\n')
    if retstr == 'None' or not retstr:
        return -1
    return float(retstr)

metrics = { 
    "leveldb-get-count" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.get-requests.count)",
    "leveldb-put-count" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.put-requests.count)",
    "leveldb-total-mean-put-latency" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.leveldb-put-latencies.mean)",
    "leveldb-total-mean-get-latency" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.leveldb-get-latencies.mean)",
    "leveldb-total-99.9th-put-latency" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.leveldb-put-latencies.999percentile)",
    "leveldb-total-99.9th-get-latency" : 
        "sumSeries(*.edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine.leveldb-get-latencies.999percentile)",
    "antientropy-ack-txn-pending-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServiceHandler.ack-transaction-pending-requests.count)",
    "antientropy-put-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServiceHandler.put-requests.count)",
    "antientropy-router-write-announce-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter.write-announce-events.count)",
    "antientropy-router-write-forward-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter.write-forward-events.count)",
    "dep-resolver-dgood-txn-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.dependencies.DependencyResolver.dgood-transaction-total.count)",
    "dep-resolver-pending-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.dependencies.DependencyResolver.num-pending-versions.value)",
    "replica-service-get-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler.get-requests.count)",
    "replica-service-put-count" :
        "sumSeries(*.edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler.put-requests.count)"    
}

tocheck = metrics.keys()
tocheck.sort()

for key in tocheck:
    print key, fetchone(metrics[key])
