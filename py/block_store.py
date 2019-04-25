#!/usr/bin/env python
##############################################################################


# Hang Zhang
# block_store.py
##############################################################################
import argparse
import time
from concurrent import futures

import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class BlockStore(SurfStoreBasic_pb2_grpc.BlockStoreServicer):
    def __init__(self, config):
        super(BlockStore, self).__init__()

        # key --> hash val, value --> block of data
        self.block_map = {}
        # self.config = config
        # self.mstub = None

    def Ping(self, request, context):
        return SurfStoreBasic_pb2.Empty()

    # ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~#

    # // Store the block in storage.
    # // The client must fill both fields of the message.
    # rpc StoreBlock (Block) returns (Empty) {}
    def StoreBlock(self, block, context):
        # print 'storing block with hash:', block.hash 
        self.block_map[block.hash] = block.data.decode()
        return SurfStoreBasic_pb2.Empty()

    # // Get a block in storage.
    # // The client only needs to supply the "hash" field.
    # // The server returns both the "hash" and "data" fields.
    # // If the block doesn't exist, "hash" will be the empty string.
    # // We will not call this rpc if the block doesn't exist (we'll always
    # // call "HasBlock()" first
    # rpc GetBlock (Block) returns (Block) {}
    def GetBlock(self, request, context):
        builder = SurfStoreBasic_pb2.Block()
        try:
            data = self.block_map[request.hash]
            builder.data = data.encode()
            builder.hash = request.hash
        except:
            print ("No mapping found for hash",request.hash)
        return builder

    # // Check whether a block is in storage.
    # // The client only needs to specify the "hash" field.
    # rpc HasBlock (Block) returns (SimpleAnswer) {}
    def HasBlock(self, block, context):
        # print 'testing for existence of block with hash:', block.hash 
        if block.hash in self.block_map:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=True)

        return SurfStoreBasic_pb2.SimpleAnswer(answer=False)

    # ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~#


def parse_args():
    parser = argparse.ArgumentParser(description="BlockStore server for SurfStore")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    parser.add_argument("-t", "--threads", type=int, default=10,
                        help="Maximum number of concurrent threads")
    return parser.parse_args()


def serve(args, config):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads))
    SurfStoreBasic_pb2_grpc.add_BlockStoreServicer_to_server(BlockStore(config), server)
    server.add_insecure_port("127.0.0.1:%d" % config.block_port)
    server.start()
    print("Server started on 127.0.0.1:%d" % config.block_port)
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)
    serve(args, config)
