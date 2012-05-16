import logging
import traceback
import threading
import Queue
import time
import csv
import socket
import random
import os.path

import paramiko
from libcloud.compute.types import Provider, NodeState
from libcloud.compute.providers import get_driver
from libcloud.compute.deployment import ScriptDeployment


from ssh_utils import CloudSSHClient

#import libcloud.security
#libcloud.security.VERIFY_SSL_CERT = False

#LOGFORMAT = '%(asctime)-15s %(name)s %(levelname)-5s %(message)s'
# debug one
LOGFORMAT = '%(asctime)-15s %(levelname)-4s %(name)s(%(threadName)s) %(funcName)s %(message)s'
logging.basicConfig(format=LOGFORMAT)

logger = logging.getLogger("c")
#logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)

from config import *

jobs_queue = None

counter_success = 0
counter_failed = 0
counter_nodes = 0


def paramiko_debug_on():
    import paramiko
    paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)


def load_jobs_queue(filename):
    f = csv.reader(open(filename, "r"))
    n=1
    for row in f:
        jobs_queue.put((n, row[0].strip(), row[1].strip(), row[2].strip(),
                        row[3].strip()))
        n += 1


def generate_jobs():
    for i in range(JOBS_NUMBER):
        jobs_queue.put((i+1, "res" + str(i), JOBS_CMD))
        

def ssh_client_for_node(node, info=""):
    if EC2_USE_INTERNAL_IP:
        hostname = node.private_ips[0]    
    else:
        hostname = node.public_ips[0]
    
    SSH_CONNECT_MAX_ATTEMPTS = 10
    for i in range(SSH_CONNECT_MAX_ATTEMPTS):
        ssh_client = CloudSSHClient(
                        hostname=hostname,
                        port=SSH_PORT, username=SSH_USERNAME,
                        key=SSH_KEYPATH)
        stime = random.randint(i*2, i*6)
        try:
            ssh_client.connect()
            break
        except socket.error, e:
            if i > 5:
                logger.warning("ssh_client.connect() socket.error for %s. Ntry: %d of %d. Sleep for %s" % (
                                            info, i, SSH_CONNECT_MAX_ATTEMPTS, stime))
            time.sleep(stime)
        except EOFError, e:
            if i > 5:
                logger.warning("ssh_client.connect() EOFError for %s. Ntry: %d of %d. Sleep for %s" % (
                                            info, i, SSH_CONNECT_MAX_ATTEMPTS, stime))
            time.sleep(stime)
        except Exception, e:
            logger.exception("%s. Level: %s" % (info, i))
            raise
    else:
        logger.exception("ssh_client.connect() persistent problem for %s after %s tries" % (
                                info, SSH_CONNECT_MAX_ATTEMPTS))
        raise

    return ssh_client


def do_work(node, job):    
    job_id, job_model, job_input, job_odir, job_args = job
    #job_id = threading.current_thread().ident

    job_cmd = JOBS_CMD.format(job_args=job_args)
    remote_dir = "/home/ubuntu/{job_id}".format(job_id=job_id)
    
    # compute dirs on the remote machine
    r_odir = '%s/output' % remote_dir
    
    params = {'job_odir': job_odir,
              'job_cmd': job_cmd,
              'job_id': job_id,
              'remote_dir': remote_dir,
              'r_odir': r_odir,}

    ssh_client = ssh_client_for_node(node,
                    info="Node: %s%s Job: %s" % (node.id, node,  job))
    
    # upload
    logger.info("%s Uploading..." % job_id)
    """
    for dirs in UPLOADS:
        assert len(dirs) == 2
        ssh_client.put_dir_recursively(dirs[0].format(**params),
                        dirs[1].format(**params))
    """
    ssh_client.put_dir_recursively(job_model, os.path.join(remote_dir, "model"))
    ssh_client.put_dir_recursively(job_input, os.path.join(remote_dir, "input"))
    logger.info("%s Uploaded" % job_id)
        
    # run
    logger.info("%s Running..." % job_id)
    cmd = "rm -rf {r_odir} && mkdir -p {r_odir} && cd {r_odir} && {job_cmd}".format(
                    **params)
    so, se, status = ssh_client.run(cmd)
    if status == 0:
        logger.info("%s Done. Status: %s" % (job_id, status))
    else:
        logger.info("%s Done. Status: %s . Stdout: %s Stderr: %s" % (job_id, status, so, se))
   
    # download
    logger.info("%s Downloading..." % job_id)
    """
    for dirs in DOWNLOADS:
        assert len(dirs) == 2
        ssh_client.get_dir_recursively(dirs[0].format(**params),
                        dirs[1].format(**params))
    """
    ssh_client.get_dir_recursively(os.path.join(remote_dir, "output"), job_odir)
    logger.info("%s Downloaded" % job_id)
    ssh_client.close()


"""
def do_all(job):
    conn = get_libcloud_conn()
    try:
        # activate
        node = libcloud_create(conn)
        #nsec = 5
        #logger.info("Waiting for extra %s sec" % nsec)
        #time.sleep(nsec)
        do_work(node, job)
#    except Exception, e:
#        raise
    finally:
        # destory
        logger.info("Destroing node %s" % node)
        node.destroy()
        logger.info("Destroyed node %s" % node)
"""


def worker_watchdog(worker):
    pass


def worker(node):
    "per node worker"
    logger.info("Worker thread for node %s%s" % (node.id, node))
    global counter_success, counter_failed
    try:
        conn = get_libcloud_conn()
        # commented because STUPID bug in libcloud/ec2 api
        #logger.info("Waiting for node %s" % node)
        #node = libcloud_node_waiting(conn, node)
        #logger.info("Node %s is ready" % node)
        
        while True:
            try:
                job = jobs_queue.get(block=False)
            except Queue.Empty:
                #libcloud_node_destroy(conn, node)
                return
    
            try:
                do_work(node, job)
                jobs_queue.task_done()
                counter_success += 1
            except paramiko.SSHException, e:
                # put failed job back to queue
                #logger.info("Put job %s back to jobs_queue" % job)
                #jobs_queue.put(job)
                logger.error("paramiko.SSHException. raise", exc_info=1)
                counter_failed += 1
                raise
            except Exception, e:
                logger.error("unknown Exception with type %s. raise" % type(e), exc_info=1)
                counter_failed += 1
                raise
    finally:
        libcloud_node_destroy(conn, node)
        #pass
    
"""
def run_in_threads(nodes):
    for node in nodes:
        t = threading.Thread(target=worker, args=(node,))
        #t.daemon = True
        t.start()
"""

def get_libcloud_conn():
    Driver = get_driver(EC2_REGION)
    conn = Driver(EC2_ACCESS_ID, EC2_SECRET)
    return conn


def libcloud_get_node_by_id(conn, nid):
    "conn.list_nodes(ex_node_ids=...) bug workaround"
    
    nodes = [i for i in conn.list_nodes() if i.id == nid]
    if len(nodes) == 0:
        return None
    elif len(nodes) == 1:
        return nodes[0]
    else:
        raise RuntimeError("2+ nodes for same nodeid %s" % nid)
    
"""
def libcloud_start():
    
    conn = get_libcloud_conn()
    
    # a simple script to install puppet post boot, can be much more complicated.
    script = ScriptDeployment("apt-get -y install puppet")
    
    image = [i for i in conn.list_images() if i.id == EC2_AMI][0]
    size = [s for s in conn.list_sizes() if s.id == EC2_TYPE][0]
    
    try:
    # deploy_node takes the same base keyword arguments as create_node.
        node = conn.deploy_node(name='test', image=image, size=size,
            #deploy=script,
                            
            ssh_port = SSH_PORT,
            ssh_username = SSH_USERNAME,
            ssh_key=SSH_KEYPATH,
            ex_keyname=EC2_KEYNAME,
            
            #ssh_timeout = 3000,
            max_tries = 1
            )
    # <Node: uuid=..., name=test, state=3, public_ip=['1.1.1.1'], provider=EC2 ...>
    # the node is now booted, with puppet installed.
    
    except Exception,e:
        traceback.print_exc()
        print e.__dict__
        raise


def libcloud_create(conn, count = 1):
    logger.info("Launching...")
    image = [i for i in conn.list_images() if i.id == EC2_AMI][0]
    assert image
    size = [s for s in conn.list_sizes() if s.id == EC2_TYPE][0]
    assert size

    node = conn.create_node(
        name='claunch', image=image, size=size,
        ex_keyname=EC2_KEYNAME,
        ex_mincount = count,
        ex_maxcount = count
    )
    node = conn._wait_until_running(node) # TODO: timeout ?
    assert node.state == 0 # must be
    
    logger.info("Launched node %s" % node)
    return node


def libcloud_node_waiting(conn, node):
    node = conn._wait_until_running(node)
    assert node.state == 0
    return node

def libcloud_nodes_waiting(conn, nodes):

    def newnodes_by_oldnodes(newnodes, oldnodes):
        def new_by_old(nodes, node):
            node_ = filter(lambda n:n.id == node.id, nodes)
            assert len(node_) == 1
            return node_[0]
        return [new_by_old(newnodes, node) for node in oldnodes]

    def check_nodes(nodes):
        for node in nodes:
            if not (node.public_ips and node.state == NodeState.RUNNING):
                return False
        return True

    logger.info("Waiting for nodes")
    for i in range(15):
        newnodes = newnodes_by_oldnodes(
                            conn.list_nodes(), nodes)

        if not check_nodes(newnodes):
            time.sleep(10)
            continue
        
        logger.info("Nodes are ready")
        return newnodes
    else:
        logger.error("Nodes are NOT ready")
        raise RuntimeError("nodes still not running")


def terminate_all():
    def terrm(node):
        conn = get_libcloud_conn()
        conn.destroy_node(node)
        
    conn = get_libcloud_conn()
    nodes = conn.list_nodes()[:2]
    nodes = filter(lambda n: n.extra['instancetype'] == "t1.micro" and n.state==0, nodes)
    print nodes
    for node in nodes:
        t = threading.Thread(target=terrm, args=(node,))
        #t.daemon = True
        t.start()
"""



def libcloud_nodes_launching(conn, count, node_name='claunch2'):
    assert count > 1
    logger.info("Launching %s nodes..." % count)
    image = [i for i in conn.list_images() if i.id == EC2_AMI][0]
    assert image
    size = [s for s in conn.list_sizes() if s.id == EC2_TYPE][0]
    assert size

    nodes = conn.create_node(
        name=node_name, image=image, size=size,
        ex_keyname=EC2_KEYNAME,
        ex_mincount = str(count),
        ex_maxcount = str(count)
    )

    if count == 1:
        nodes = [nodes]

    logger.info("Launched %s nodes" % count)
    return nodes


def waiting_n_run_in_threads(conn, nodes, lev=1):

    def newnode_by_old(nodes, node):
        node_ = filter(lambda n:n.id == node.id, nodes)
        assert len(node_) == 1
        return node_[0]

    if lev > 30:
        #raise RuntimeError("Problem with nodes %s" % nodes)
        logger.critical("Unwaited %d nodes" % len(nodes))
        for node in nodes:
            logger.critical("Problem with %s%s" % (node.id, node))
        return nodes

    logger.info("Waiting for %s nodes... Step: %s" % (len(nodes), lev))
    if lev > 5 and len(nodes) <= 5:
        for node in nodes:
            logger.debug("Waiting for node %s%s" % (node.id, node))            

    nextnodes = []
    newnodes = conn.list_nodes()

    for node in nodes:
        newnode = newnode_by_old(newnodes, node)
        # launch a thread when node gives up
        if newnode.public_ips and newnode.state == NodeState.RUNNING:
            t = threading.Thread(target=worker, args=(newnode,))        
            t.start()
        else:
            nextnodes.append(node)
    logger.debug("Last node info %s%s for status" % (node.id, node))            
        
    if nextnodes:
        time.sleep(10)
        return waiting_n_run_in_threads(conn, nextnodes, lev=lev+1) # tail recursion
    
    return None


def libcloud_node_destroy(conn, node):
    logger.info("Destroing node %s%s" % (node.id, node))
    try:
        if conn.destroy_node(node):
            global counter_nodes
            counter_nodes -= 1
            logger.info("Destroyed node %s%s" % (node.id, node))
        else:
            logger.critical("Some problem with destroing node %s%s" % (node.id, node))
    #except RequestLimitExceeded:
    except Exception, e:
        if len(e.args) == 1 and e.args[0] == "RequestLimitExceeded: Request limit exceeded.":
            logger.exception("RequestLimitExceeded: Request limit exceeded. for node %s%s. Retrying" % (
                                                                            node.id, node))
            time.sleep(random.randint(5, 15))
            return libcloud_node_destroy(conn, node)

        logger.exception("Unk exc %s" % type(e))
        raise


def libcloud_destroy_nodes(conn, nodes):
    #map(lambda x: x.destroy(), nodes)
    for node in nodes:
        libcloud_node_destroy(conn, node)



def launch_nodes():
    max_nodes = min(jobs_queue.qsize(), MAX_NODES)
    conn = get_libcloud_conn()
    nodes = libcloud_nodes_launching(conn, max_nodes)
    # experiment !
    #if nodes[0].state == 2:
    #    raise RuntimeError("Server.InsufficientInstanceCapacity: Insufficient capacity to satisfy instance request")
    global counter_nodes
    counter_nodes += len(nodes)
    badnodes = waiting_n_run_in_threads(conn, nodes)
    if badnodes:
        libcloud_destroy_nodes(conn, badnodes)


def main():
        
    global jobs_queue
    jobs_queue = Queue.Queue()

    load_jobs_queue(JOBS_FILENAME)
    #generate_jobs()
    
    launch_nodes()
    
    #run_in_threads(nodes)

    while True:
        njobs = jobs_queue.qsize()
        nthreads = threading.active_count() - 1
        logger.info("Success: %d Fail: %d Jobs queue size: %d Nodes: %d Threads: %s" % (
                                counter_success, counter_failed, njobs, counter_nodes, nthreads))
        if njobs == 0 and nthreads == 0:
            break
        #if counter_nodes == 0 and njobs > 0:
        #    launch_nodes()
        time.sleep(15)
    logger.info("Finish")


if __name__ == "__main__":
    #terminate_all()
    main()
    #test1()
    #test2()
    
