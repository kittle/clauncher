#clauncher
=========

Launch N nodes(ec2 instances) and run jobs on them in a very sipmple, straightforward way via ssh.

Can launch and run jobs on up to 500 ec2 instances. Better use c1.medium node for claunch.



### Lifecycle

*  load job list
*  launch instances
*  for each instance
* *  waiting for node 
* *  upload data
* *  run job via ssh
* *  download result


### INSTALL


`apt-get install python-paramiko python-libcloud`

or

`pip install paramiko apache-libcloud`


### TODO

* improve launch logic to keep up to MAX_NODES 
