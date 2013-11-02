
from os import system
from time import sleep


started = []

def setup(hostfile):
    servers = []

    for line in open(hostfile):
        servers.append(line[:-1].split(":"))

    for server1 in servers:
        for server2 in servers:
            if server1 == server2:
                continue
            print server1[0]+"-"+server2[0]
            
            f = open("/tmp/run.sh", 'w')
            f.write("bash -c 'ping %s 2>&1 | while read pong; do echo \"$(date -u +%%s): $pong\"; done' 2>&1  %s-%s-times.txt" % (server2[1], server1[0], server2[0]))
            f.close()
            if server1 not in started:
                system("ssh -o StrictHostKeyChecking=no ec2-user@%s 'pkill -9 ping; pkill -9 bash'" % (server1[1]))
                started.append(server1)
            system("scp -o StrictHostKeyChecking=no /tmp/run.sh ec2-user@%s:run.sh; ssh -StrictHostKeyChecking=no ec2-user@%s 'sh -c \"( (nohup bash run.sh 2>&1 >%s-%s-times.txt </dev/null) & )\"' &" % (server1[1], server1[1], server1[0], server2[0]))



setup("region-hosts.txt")
setup("az-hosts.txt")
setup("eastb-hosts.txt")
system("pkill -9 ssh")
