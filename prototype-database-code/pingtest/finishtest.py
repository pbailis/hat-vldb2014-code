
from os import system
from time import sleep


started = []

def setup(hostfile):
    servers = []

    for line in open(hostfile):
        servers.append(line[:-1].split(":"))

    for server1 in servers:
        server1 = server1[1]
        print("ssh -o StrictHostKeyChecking=no ec2-user@%s 'pkill -9 bash ping'" % (server1))
        print("scp -C -o StrictHostKeyChecking=no ec2-user@%s:*.txt output/" % (server1))

        system("ssh -o StrictHostKeyChecking=no ec2-user@%s 'pkill -9 bash ping'" % (server1))
        system("scp -C -o StrictHostKeyChecking=no ec2-user@%s:*.txt output/" % (server1))

setup("region-hosts.txt")
setup("az-hosts.txt")
setup("eastb-hosts.txt")
