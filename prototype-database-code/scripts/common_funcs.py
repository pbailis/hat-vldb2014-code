# Common helper functions

import subprocess
from os import system
from time import sleep

def run_cmd(hosts, cmd, user="root"):
    print("parallel-ssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"%s\"" % (user, hosts, cmd))
    system("parallel-ssh -t 1000 -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt \"ulimit -u unlimited; %s\"" % (user, hosts, cmd))

def run_cmd_single(host, cmd, user="root"):
    print("ssh -o StrictHostKeyChecking=no %s@%s \"%s\"" % (user, host, cmd))
    system("ssh -o StrictHostKeyChecking=no %s@%s \"ulimit -u unlimited; %s\"" % (user, host, cmd))

def run_process_single(host, cmd, user="root", stdout=None, stderr=None):
    subprocess.call("ssh %s@%s \"%s\"" % (user, host, cmd),
                    stdout=stdout, stderr=stderr, shell=True)

def upload_file(hosts, local_path, remote_path, user="root"):
    system("cp %s /tmp" % (local_path))
    script = local_path.split("/")[-1]
    system("parallel-scp -O StrictHostKeyChecking=no -l %s -h hosts/%s.txt /tmp/%s %s" % (user, hosts, script, remote_path))

def run_script(hosts, script, user="root"):
    upload_file(hosts, script.split(" ")[0], "/tmp", user)
    run_cmd(hosts, "bash /tmp/%s" % (script.split("/")[-1]), user)

def fetch_file_single(host, remote, local, user="root"):
    system("scp %s@%s:%s '%s'" % (user, host, remote, local))

def fetch_file_single_compressed(host, remote, local, user="root"):
    system("scp %s@%s:%s '%s'" % (user, host, remote, local))

def get_host_ips(hosts):
    return open("hosts/%s.txt" % (hosts)).read().split('\n')[:-1]
        
def sed(file, find, repl):
    iOpt = ''
    print 'sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file)
    system('sed -i -e %s \'s/%s/%s/g\' %s' % (iOpt, escape(find), escape(repl), file))

def escape(path):
    return path.replace('/', '\/')

def get_node_ips():
    ret = []
    system("ec2-describe-instances > /tmp/instances.txt")
    system("ec2-describe-instances --region us-west-2 >> /tmp/instances.txt")
    for line in open("/tmp/instances.txt"):
        line = line.split()
        if line[0] != "INSTANCE" or line[5] != "running":
            continue
        # addr, externalip, internalip, ami
        ret.append((line[3], line[13], line[14], line[1]))
    return ret

def get_matching_ip(host, hosts):
    cips = get_host_ips(hosts)
    #argh should use a comprehension/filter; i'm tired
    for h in get_node_ips():
        if h[0] == host:
            return h[1]
