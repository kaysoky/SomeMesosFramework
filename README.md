Some Mesos Framework
====================

Solution for [Project Euler #52](https://projecteuler.net/problem=52)

### Requirements

- [VirtualBox](http://www.virtualbox.org/) 4.1.18+
- [Vagrant](http://www.vagrantup.com/) 1.3+

### Start the `mesos-demo` VM

```bash
$ wget http://downloads.mesosphere.io/demo/mesos.box -O mesos.box
$ vagrant box add --name mesos-demo mesos.box
$ vagrant up
```

The Mesos Web UI can be viewed here:
[http://10.141.141.10:5050](http://10.141.141.10:5050)

### Run framework in the `mesos-demo` VM

```bash
$ vagrant ssh
$ cd hostfiles

# Update install dependencies
$ sudo apt-get update
$ sudo apt-get install libcurl4-openssl-dev libboost-regex1.55-dev \
    libprotobuf-dev libgoogle-glog-dev protobuf-compiler

# Build
$ make all

# Start the scheduler with the mesos master ip
$ ??? --master 127.0.1.1:5050
# <Ctrl+C> to stop...
```

### Cleanup

```bash
# Exit out of the VM
vagrant@mesos:hostfiles $ exit

# Stop the VM
$ vagrant halt

# To delete all traces of the vagrant machine
$ vagrant destroy
```
