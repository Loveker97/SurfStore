##############################################################################
# Project 2 - SurfStore
# Peter Panourgias
# Feng Jiang
# unittester.py
##############################################################################
#!/usr/bin/env python
from __future__ import print_function
import base64
import hashlib

import argparse
import os.path
import random
import string

import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader
import sys

##############################################################################

# PROFESSORS TESTS

def test_blockserver(mstub, bstub):
    b1 = SurfStoreBasic_pb2.Block(hash=sha256('block_01'), data='block_01')
    b2 = SurfStoreBasic_pb2.Block(hash=sha256('block_02'), data='block_02')

    assert bstub.HasBlock(b1).answer == False
    assert bstub.HasBlock(b2).answer == False

    bstub.StoreBlock(b1)
    assert bstub.HasBlock(b1).answer == True

    bstub.StoreBlock(b2)
    assert bstub.HasBlock(b2).answer == True

    b1_prime = bstub.GetBlock(b1)
    assert b1_prime.hash == b1.hash
    assert b1_prime.data == b1.data

    b2_prime = bstub.GetBlock(b2)
    assert b2_prime.hash == b2.hash
    assert b2_prime.data == b2.data

    return 'test_blockserver == PASS'

def test_md_centralized_filenotfound(mstub):
    result = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename='notfound.txt'))
    assert result.filename == 'notfound.txt'
    assert result.version == 0

    return 'test_md_centralized_filenotfound == PASS'

def test_md_centralized_missingblocks(mstub, bstub):
    cat_b0 = 'cat_block0'
    cat_b1 = 'cat_block1'
    cat_b2 = 'cat_block2'

    cathashlist = [ sha256(b) for b in [cat_b0, cat_b1, cat_b2] ]

    file_info = SurfStoreBasic_pb2.FileInfo(
        blocklist=cathashlist,
        filename='cat.txt',
        version=1
    )

    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 2 # MISSING_BLOCKS
    assert len(write_result.missing_blocks) == 3

    bstub.StoreBlock(SurfStoreBasic_pb2.Block(hash=cathashlist[0], data=cat_b0))
    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 2 # MISSING_BLOCKS
    assert len(write_result.missing_blocks) == 2

    bstub.StoreBlock(SurfStoreBasic_pb2.Block(hash=cathashlist[1], data=cat_b1))
    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 2 # MISSING_BLOCKS
    assert len(write_result.missing_blocks) == 1

    bstub.StoreBlock(SurfStoreBasic_pb2.Block(hash=cathashlist[2], data=cat_b2))
    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 0 # OK
    assert len(write_result.missing_blocks) == 0

    return 'test_md_centralized_missingblocks == PASS'

# END PROFESSORS TESTS

def mod_readfile_nofile(mstub, bstub):
    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename='nofile.txt'))

    assert file_info.filename  == 'nofile.txt'
    assert file_info.version   == 0
    assert file_info.blocklist == []

    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename=''))

    assert file_info.filename  == ''
    assert file_info.version   == 0
    assert file_info.blocklist == []

    return 'mod_readfile_nofile == PASS'

def modify_file_initial_test(mstub, bstub):
    file_info = SurfStoreBasic_pb2.FileInfo( 
        filename='testfiles/code',
        version=0,
        blocklist=[]
    )

    result = mstub.ReadFile(file_info)

    missing_blocks = create_blocklist(file_info.filename)
    file_info.blocklist[:] = missing_blocks.keys()
    write_result = mstub.ModifyFile(file_info)

    assert write_result.result == 1 #OLD_VERSION

    file_info.version = 1
    write_result = mstub.ModifyFile(file_info)

    assert write_result.result == 2 #MISSING_BLOCKS
    assert write_result.current_version == 0
    assert sorted(write_result.missing_blocks) == sorted(missing_blocks.keys())

    return ('modify_file_initial_test == PASS', missing_blocks)

def store_to_bs_test_init(mstub, bstub, mb):
    for req in mb.keys():
        to_add = SurfStoreBasic_pb2.Block(hash=req,data=mb[req])
        assert not bstub.HasBlock(to_add).answer
        bstub.StoreBlock(to_add)

    for req in mb.keys():
        to_add = SurfStoreBasic_pb2.Block(hash=req)
        assert bstub.HasBlock(to_add).answer
        assert bstub.GetBlock(to_add).data == mb[req]

    for req in mb.keys():
        to_add = SurfStoreBasic_pb2.Block(hash='')
        assert not bstub.HasBlock(to_add).answer
        assert bstub.GetBlock(to_add).data == ''
        assert bstub.GetBlock(to_add).hash == ''

    for req in mb.keys():
        to_add = SurfStoreBasic_pb2.Block(hash=req + 'z')
        assert not bstub.HasBlock(to_add).answer
        assert bstub.GetBlock(to_add).data == ''
        assert bstub.GetBlock(to_add).hash == ''

    return 'store_to_bs_test_init == PASS'

def del_tests(mstub, bstub):
    cat_b0 = 'tester.1'
    cat_b1 = 'tester.2'
    cat_b2 = 'tester.3'
    datalist = [cat_b0, cat_b1, cat_b2]

    cathashlist = [ sha256(b) for b in datalist ]

    for _hash,_data in zip(cathashlist,datalist):
        bl = SurfStoreBasic_pb2.Block(hash=_hash,data=_data)
        bstub.StoreBlock(bl)

    file_info = SurfStoreBasic_pb2.FileInfo(
        blocklist=cathashlist,
        filename='tester.txt',
        version=1
    )

    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 0 # OK

    file_info_read = mstub.ReadFile(file_info)

    assert file_info_read.filename == 'tester.txt'
    assert file_info_read.version == 1
    assert sorted(file_info_read.blocklist) == sorted(cathashlist)

    write_result = mstub.DeleteFile(SurfStoreBasic_pb2.FileInfo(filename='tester.txt',version=1))

    assert write_result.result == 1 # OLD_VERSION

    file_info_read = mstub.ReadFile(file_info)

    assert file_info_read.filename == 'tester.txt'
    assert file_info_read.version == 1
    assert sorted(file_info_read.blocklist) == sorted(cathashlist)

    write_result = mstub.DeleteFile(SurfStoreBasic_pb2.FileInfo(filename='tester.txt',version=2))

    assert write_result.result == 0 # OK

    file_info_read = mstub.ReadFile(file_info)

    assert file_info_read.filename == 'tester.txt'
    assert file_info_read.version == 2
    assert file_info_read.blocklist == ['0']

    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 1 # OLD_VERSION

    file_info.version = 2

    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 1 # OLD_VERSION

    datalist = []

    for i in range(5000):
        datalist.append(''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(1000)))

    hashlist = [ sha256(b) for b in datalist ]

    for _hash,_data in zip(hashlist,datalist):
        bl = SurfStoreBasic_pb2.Block(hash=_hash,data=_data)
        bstub.StoreBlock(bl)

    file_info = SurfStoreBasic_pb2.FileInfo(
        blocklist=hashlist,
        filename='tester.txt',
        version=3
    )

    write_result = mstub.ModifyFile(file_info)
    assert write_result.result == 0 # OK

    file_info_read = mstub.ReadFile(file_info)

    assert file_info_read.filename == 'tester.txt'
    assert file_info_read.version == 3
    assert sorted(file_info_read.blocklist) == sorted(hashlist)

    return 'del_tests == PASS'

##############################################################################

def sha256(s):
    m = hashlib.sha256()
    m.update(s.encode('utf-8'))
    return base64.b64encode(m.digest())

def create_blocklist(filename):
    try:
        file = open(filename, 'rb')
        block_map = {}

        while True:
            block = file.read(4096)
            read_amt = len(block)

            if read_amt != 0:
                block_map[sha256(block)] = block
            else:
                break

        return block_map

    except IOError: return None

def parse_args():
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    return parser.parse_args()


def get_metadata_stub(config):
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[1])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    return stub


def get_block_stub(config):
    channel = grpc.insecure_channel('localhost:%d' % config.block_port)
    stub = SurfStoreBasic_pb2_grpc.BlockStoreStub(channel)
    return stub


def run(config):
    metadata_stub = get_metadata_stub(config)
    block_stub = get_block_stub(config)

    metadata_stub.Ping(SurfStoreBasic_pb2.Empty())
    print("Successfully pinged the Metadata server")

    block_stub.Ping(SurfStoreBasic_pb2.Empty())
    print("Successfully pinged the Blockstore server")
    
    print("Starting the client interface. . .")
    print("BEGIN UNIT TESTS")

    result = mod_readfile_nofile(metadata_stub, block_stub)
    print(result)
    result = modify_file_initial_test(metadata_stub, block_stub)
    print(result[0])
    result = store_to_bs_test_init(metadata_stub, block_stub, result[1])
    print(result)
    result = test_md_centralized_missingblocks(metadata_stub, block_stub)
    print(result)
    result = test_md_centralized_filenotfound(metadata_stub)
    print(result)
    result = test_blockserver(metadata_stub, block_stub)
    print(result)
    result = del_tests (metadata_stub, block_stub)
    print(result)

if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)
    run(config)