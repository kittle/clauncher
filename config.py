import socket
from libcloud.compute.types import Provider

JOBS_FILENAME = "jobs.txt"

MAX_NODES = 100 # 425
#JOBS_NUMBER = 2 # 425
JOBS_CMD = "python ../model/run.py {job_args} 2>&1 >run.log"


EC2_ACCESS_ID = '*SET_THIS*'
EC2_SECRET = '*SET_THIS*'
EC2_REGION = Provider.EC2_US_EAST
EC2_TYPE = 't1.micro'
EC2_AMI = '*SET_THIS*' # 'ami-a29943cb' http://cloud.ubuntu.com/ami/
# Name of the SSH key which is automatically installed on the server.
# Key needs to be generated and named in the AWS control panel.
EC2_KEYNAME = 'ec2-keypair'
EC2_USE_INTERNAL_IP = True

SSH_PORT = 22 # 80
SSH_USERNAME = 'ubuntu'
# Path to the private key created using the AWS control panel.
SSH_KEYPATH = '../keys/ec2-keypair.pem'


#UPLOADS = (('/home/ubuntu/for-andrey/model', '{remote_dir}/model'),
#       ('/home/ubuntu/for-andrey/model', '{remote_dir}/input'))

#DOWNLOADS = (('{remote_dir}/output', '{job_odir}'),)

