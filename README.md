
## Overview
This is a cloud-based file storage application that supports creation, read, update, and deletion of a file. The communications among the servers and clients are through the Google Protocol RPC API. The application allows concurrent clients connection and has fault tolerance with a set of distributed processes. The distributed processes are coordinated with the ideas of replicated state machine and two phase commit. <br />
The storage application consists of metadata_store and block_store. The metadata_store holds the mapping of filenames to data blocks while the block_store is where the data blocks actually stored. When received a request from the client, the metadata_store divides the data of the file into blocks and only sends the data blocks that are modified to the block_store so as to save some space. Currently the metadata_store can support fault tolerant so there will be multiple metadata_stores running and the shutdown of a minority of them won't affect the performance of the application.

## To build the code:

$ ./compile_protobuf.sh

## The config file

The config file specifies the number of metadata_store servers, the id of the leader server, and the port number of all the server. 

## To run the services:

$ metadata_store.py [-h] [-n NUMBER] [-t THREADS] config_file
$ block_store.py [-h] [-t THREADS] config_file

## To run the client

$ client.py [-h] config_file

## Possible future improvements

1. Implement fault tolerance also for the block_store.
2. Currently the block_store is assumed to not fail and stores data in memory, I should implement functionality in the block_store to periodically store data persistently to its disk.

## Authors

Hang Zhang
