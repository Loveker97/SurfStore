##############################################################################
# Project 2 - SurfStore
# Peter Panourgias
# Feng Jiang
# client.py
##############################################################################
#!/usr/bin/env python
from __future__ import print_function
import base64
import hashlib

import argparse
import os.path

import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader


##############################################################################

def sha256(s):
    m = hashlib.sha256()
    m.update(str.encode(s))
    return base64.b64encode(m.digest())

def create_blocklist(filename):
    try:
        file = open(filename, 'rb')
        hash_block_tups = []

        while True:
            block = file.read(4096)
            read_amt = len(block)

            if read_amt != 0:
                _hash = sha256(block)
                hash_block_tups.append( (_hash, block) )
            else:
                break

        return hash_block_tups

    except IOError: return None

def _create(mstub, bstub, filename, ver):
    # Get the current file info if it exists
    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename=filename))

    # Create can only be used if the file never exist or was deleted, else use _modify
    # If was deleted
    if file_info.version != 0:
        remote_ver = file_info.version
        # Fail if file currently exists
        if file_info.blocklist[0] != '0': #deleted file has a hashlist with a single hash value of '0'
            print("File already exists w/ version #" + str(file_info.version))
            return
        # Fail if file version which user provided is smaller than v+1
        if ver != remote_ver + 1:
            print("File version incorrect w/ remote version #" + str(file_info.version))
            return
        
    # Set the request version
    file_info.version = ver
    
    modifyFile(file_info, mstub, bstub)

def _modify(mstub, bstub, filename, ver): 
    # Get the current file info if it exists
    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename=filename))
    remote_ver = file_info.version
    if remote_ver == 0:
        print("FAILED, file doesn't exit")
        return
    if ver != remote_ver + 1:
        print("File version incorrect w/ remote version #" + str(file_info.version))
        return
    # we shouldn't be reusing _create here, because it modifies file only if it didn't exist or was deleted, while _modify needs to modify file that alredy exists so it might be better to have a helper function
    # Set the request version
    file_info.version = ver
    
    modifyFile(file_info, mstub, bstub)
    
    
def modifyFile(file_info, mstub, bstub):
    filename = file_info.filename
    # Get the list of hashes
    hash_block_tups = create_blocklist(filename)

    # If this returns None, then the file is not in this directory
    if hash_block_tups == None:
        print("Filename: " + filename + ", does not exist!")
        return

    file_info.blocklist[:] = [ x[0] for x in hash_block_tups ] # List of hashes
    
    result = mstub.ModifyFile(file_info)

    if result.result == 2: # MISSING_BLOCKS
        block_map = dict(hash_block_tups)

        for req in result.missing_blocks:
            try:
                to_add = SurfStoreBasic_pb2.Block(hash=req,data=block_map[req])
                bstub.StoreBlock(to_add)
            except Exception:
                print("Error during file storage, please check the blockstore connection.")
                break
                
        # the indent here was probably not needed 
        updated_result = mstub.ModifyFile(file_info)

        if updated_result.result == 0: # OK
            print("Upload successful!")
        else:
            print("Upload not successful :(")

    elif result.result == 1: # OLD_VERSION
        print("FAILED, version incorrect")
    
    elif result.result == 3:
        print("FAILED the server is not leader")

    elif result.result == 0: # OK
        print("Did not update anything, but result == OK so the data probably already existed")
    
# support reading of any meta_data store
def _read(config, bstub, filename, serverID):     
    # create the fileinfo message to send to metadata
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    mstub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename=filename))
    block_list = []

    if file_info.version == 0:
        print("FILE NOT FOUND!")
        return
    print("found %s with version # %d" % (file_info.filename, file_info.version))
    if file_info.blocklist[0] == '0':
        print("FILE WAS DELETED!")
        return
    print("downloading file")
    for block in file_info.blocklist:
        if bstub.HasBlock(SurfStoreBasic_pb2.Block(hash=block)).answer == True:
            data = bstub.GetBlock(SurfStoreBasic_pb2.Block(hash=block))
            block_list.append(data.data)
    print("writting local file")
    if len(block_list) > 0:
        out = open(filename, 'wb')

        for data in block_list:
            out.write(data)

def _delete(mstub, bstub, filename, ver): 
    # create the fileinfo message to send to metadata
    file_info = mstub.ReadFile(SurfStoreBasic_pb2.FileInfo(filename=filename))

    file_info.version = ver

    result = mstub.DeleteFile(file_info).result

    if result == 0:
        print("Deleted file successfully!")
    elif result == 1:
        print("Delete unsuccessful, old version")
    elif result == 3:
        print("Delete unsuccessful, the server is not leader")


############### part 2 ###############
def _ping(serverID, config):
    if serverID > config.num_metadata_servers:
        print("the specified server was not born")
        return
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    try:
        stub.Ping(SurfStoreBasic_pb2.Empty())
    except:
        print("can't ping server #%d" % serverID)
        return
    print("ping server #%d successfully" % serverID)

def _crash(serverID, config):
    if serverID > config.num_metadata_servers:
        print("the specified server was not born")
        return
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    stub.Crash(SurfStoreBasic_pb2.Empty())

def _restore(serverID, config):
    if serverID > config.num_metadata_servers:
        print("the specified server was not born")
        return
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    stub.Restore(SurfStoreBasic_pb2.Empty())

def _isLeader(serverID, config):
    if serverID > config.num_metadata_servers:
        print("the specified server was not born")
        return
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    answer = stub.IsLeader(SurfStoreBasic_pb2.Empty()).answer
    if answer == True:
        print("I am the leader!")
    else:
        print("I am not the leader.")

def _isCrashed(serverID, config):
    if serverID > config.num_metadata_servers:
        print("the specified server was not born")
        return
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[serverID])
    stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
    answer = stub.IsCrashed(SurfStoreBasic_pb2.Empty()).answer
    if answer == True:
        print("I am crashed.....")
    else:
        print("I am fine, thanks for asking.")
######################################


def run_user_cli(mstub, bstub, config):
    hash_set = set()

    help_dialog = '''
        HELP!
        Commands:
            ############ part 1 ############

            <create or c> <filename> <version>

            <modify or m> <filename> <version>

            <delete or d> <filename> <version>

            <read or r> <filename> <#ID of metadata server> 

            ############ part 2 ############
            <ping> <#ID of metadata server> 

            <crash> <#ID of metadata server> 

            <restore> <#ID of metadata server> 

            <isLeader> <#ID of metadata server> 

            <isCrashed> <#ID of metadata server>


            #################################

            to exit use the command <bye or b>
            enter <help or h> for help
       '''

    try:
        while True:
            print("SurfCLI(h for help)>> ", end="")
            input = str(raw_input())
            sp = input.split(" ")
            # make the interface more user friendly and corner bug free
            if len(sp) == 0:
                continue
            
            if len(sp) == 1:
                op = sp[0].lower()
                if (op == "help" or op == "h"):
                    print(help_dialog)
                    continue
                if (op == "bye" or op == "b"):
                    raise KeyboardInterrupt

   
            if len(sp) == 2:
                op = sp[0].lower()
                if op == "create" or op == "c":
                    # set version to 1 for file that is created the first time
                    _create(mstub, bstub, sp[1], 1)
                    continue
                ########## part2 ##########
                if (op == "ping"):
                    _ping(int(sp[1]), config)
                    continue
                if (op == "crash"):
                    _crash(int(sp[1]), config)
                    continue
                if (op == "restore"):
                    _restore(int(sp[1]), config)
                    continue
                if (op == "isleader"):
                    _isLeader(int(sp[1]), config)
                    continue
                if (op == "iscrashed"):
                    _isCrashed(int(sp[1]), config)
                    continue
                ###########################
                    
            if len(sp) == 3:
                op = sp[0].lower()
                if   op == "create" or op == "c":
                    _create(mstub, bstub, sp[1], int(sp[2]))
                    continue
                if op == "modify" or op == "m": 
                    _modify(mstub, bstub, sp[1], int(sp[2]))
                    continue
                if op == "delete" or op == "d": 
                    _delete(mstub, bstub, sp[1], int(sp[2]))
                    continue
                if op == "read" or op == "r": 
                    _read(config, bstub, sp[1], int(sp[2]))
                    continue
            print('invalid command')

    except KeyboardInterrupt:
        print("Ending session. . .")

##############################################################################
#debug tests
##############################################################################

def ensure(arg):
    if arg == False:
        raise Exception('ensure failed')
        
def stringToBlock(mystr):
    mydata = str((bytearray(mystr, 'UTF-8')))
    myhash = sha256(mystr)
    block = SurfStoreBasic_pb2.Block(hash = myhash, data = mydata)
    return block
    
def testBlockStore(block_stub):
    b1 = stringToBlock('block_01')
    b2 = stringToBlock('block_02')
    ensure(block_stub.HasBlock(b1).answer == False)
    ensure(block_stub.HasBlock(b2).answer == False)
    
    block_stub.StoreBlock(b1)
    ensure(block_stub.HasBlock(b1).answer == True)
    block_stub.StoreBlock(b2)
    ensure(block_stub.HasBlock(b2).answer == True)
    
    b1prime = block_stub.GetBlock(b1)
    ensure(b1prime.hash == b1.hash)
    ensure(b1prime.data == b1.data)
    
    print ('passed all the tests, yeah!')


##############################################################################

def parse_args():
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    return parser.parse_args()


def get_metadata_stub(config):
    # client read config to know who is leader and connect to it
    leaderID = config.num_leaders
    channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[leaderID])
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

    run_user_cli(metadata_stub, block_stub, config)

if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)

    run(config)
