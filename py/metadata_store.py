#!/usr/bin/env python
##############################################################################
# Project 2 - SurfStore
# Peter Panourgias
# Feng Jiang
# metadata_store.py
##############################################################################
import argparse
import time
from concurrent import futures
import grpc

import SurfStoreBasic_pb2
import SurfStoreBasic_pb2_grpc

from config_reader import SurfStoreConfigReader

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_IS_DELETED = 2
_VERS       = 0
_BL         = 1

class MetadataStore(SurfStoreBasic_pb2_grpc.MetadataStoreServicer):
    def __init__(self, config):
        super(MetadataStore, self).__init__()

        # key --> file names, value --> (version, current blocklist, isDeleted)
        self.files   = {}
        self.config  = config
        self.bstub   = None

        # 2PC
        self.distributed = (config.num_metadata_servers > 1)
        self.crashed = False
        self.leader  = False
        self.myID = 0
        # store followers by (portsID, mstub)
        self.mstub_list = []
        # store crashed followers by index in mstub_list
        self.crashed_followers = []

        # (cmd, filename, vers, blocklist)
        self.logs = []

# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~#

    def init_distributed_server(self):
        if self.distributed == True:
            self.mstub_list = self.get_metadata_stub_list(config)
            self.ServerPing()


    def get_metadata_stub_list(self, config):
        stub_list = []
        if not self.leader:
            channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[config.num_leaders])
            stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
            stub_list.append((config.num_leaders, stub))
            return stub_list

        for i in range(1, config.num_metadata_servers + 1):
            # Don't make a reference to myself
            if i == self.myID:
                continue

            channel = grpc.insecure_channel('localhost:%d' % config.metadata_ports[i])
            stub = SurfStoreBasic_pb2_grpc.MetadataStoreStub(channel)
            stub_list.append((i, stub))

        return stub_list


    def get_block_stub(self):
        ''' Copied from the client, needed to interact with the blockstore '''
        channel = grpc.insecure_channel('localhost:%d' % self.config.block_port)
        stub = SurfStoreBasic_pb2_grpc.BlockStoreStub(channel)
        return stub


    def check_blockstore_connection(self):
        # Check if the BlockStore is up and running
        if self.bstub == None:
            self.bstub = self.get_block_stub()

        try:
            self.bstub.Ping(SurfStoreBasic_pb2.Empty())
        except Exception:
            print("Error: MetadataStore could not connect to BlockStore")
            return False

        return True


    def get_missing_blocks(self, file_info):
        missing_blocks = []

        for b_hash in file_info.blocklist:
            bl = SurfStoreBasic_pb2.Block(hash=b_hash)

            if self.bstub.HasBlock(bl).answer == False:
                missing_blocks.append(b_hash)

        return missing_blocks


    def two_phase_commit(self, cmd, file_info):
        # leader log locally
        log = [cmd, file_info.filename, file_info.version, file_info.blocklist]
        self.logs.append(log)
        votes = 0
        # 1st phase of 2PC
        for i in range(len(self.mstub_list)):
            result = self.mstub_list[i][1].Vote(SurfStoreBasic_pb2.Empty())
            if result.answer == True:
                votes += 1
            else:
                if i not in self.crashed_followers:
                    self.crashed_followers.append(i)
        # 2nd phase of 2PC
        if votes >= 1.*len(self.mstub_list)/2:
            for i in range(len(self.mstub_list)):
                if i in self.crashed_followers:
                    continue
                rpc_log = SurfStoreBasic_pb2.Log(cmd = log[0], filename = log[1], version = log[2], blocklist = log[3])
                self.mstub_list[i][1].Commit(rpc_log)
            return True
        else:
            # piazza says if we don't get majority vote, we just hang on there
            # though it seems like TA won't actually test on it
            self.logs.pop()
            # check whether crashed server is up
            while(len(self.crashed_followers) > 1.*len(self.mstub_list)/2):
                for i in self.crashed_followers:
                    result = self.mstub_list[i][1].Vote(SurfStoreBasic_pb2.Empty())
                    if result.answer == True:
                        self.crashed_followers.remove(i)

            # call the function again, now we should be good to go
            self.two_phase_commit(cmd, file_info)
            return False


    def update_crashed_server(self):
        updated_list = []
        # convert to rpc logs
        rpc_logs = SurfStoreBasic_pb2.Logs()
        for log in self.logs:
            rpc_log = SurfStoreBasic_pb2.Log(cmd = log[0], filename = log[1], \
                version = log[2], blocklist = log[3])
            rpc_logs.allLogs.extend([rpc_log])
        for i in self.crashed_followers:
            result = self.mstub_list[i][1].Update(rpc_logs)
            if result.answer == True:
                updated_list.append(i)
        for i in updated_list:     
            self.crashed_followers.remove(i)
    
######################################################
################# API Calls ###########################
######################################################
    
    def ServerPing(self):
        for stub in self.mstub_list:
            try:
                stub[1].Ping(SurfStoreBasic_pb2.Empty())
            except Exception:
                continue

    def Ping(self, request, context):
        return SurfStoreBasic_pb2.Empty()


    def ReadFile(self, file_info, context):
        """ 
        Takes a FileInfo obj with only the filename field filled out and
        fills in the version and blocklist fields and returns the completed
        object.
        """
        fn = file_info.filename

        if len(fn) != 0 and fn in self.files:
            # The file name exists, update with the info
            info_tup = self.files[fn]
            file_info.version = info_tup[_VERS]
            file_info.blocklist[:] = info_tup[_BL]
            if self.files[fn][_IS_DELETED]:
                file_info.blocklist[:] = ['0']  # a deleted file has a hashlist with a single hash value of "0"
        else:
            # vers == 0 signals that the file d/n exist
            file_info.version = 0
            file_info.blocklist[:] = []
        
        return file_info


    # rpc ModifyFile (FileInfo) returns (WriteResult) {}
    def ModifyFile(self, file_info, context):
        # Use this to return the result, assume MISSING_BLOCKS
        mod_result = SurfStoreBasic_pb2.WriteResult(result=2)

        if not self.leader:
            mod_result.result = 3
            return mod_result
        
        file_tup = None

        # Check if the file already exists in the metadata record
        if file_info.filename in self.files:
            file_tup = self.files[file_info.filename]
            mod_result.current_version = file_tup[_VERS]
        else: 
            file_tup = (0,file_info.blocklist,False)

        # The case where the new vers is not current version + 1
        if file_info.version != (file_tup[_VERS] + 1):
            mod_result.result = 1 # OLD_VERSION
        # The case where the file exists and the next vers num is correct
        else:
            ###################
            # 2PC
            if self.distributed:
                if self.leader:
                    self.two_phase_commit("mod", file_info)
            ###################

            self.check_blockstore_connection()
            # Used to maintain a list of missing blocks in the blockstore
            missing_blocks = self.get_missing_blocks(file_info)
            mod_result.missing_blocks[:] = missing_blocks

            if len(missing_blocks) == 0:
                mod_result.result = 0 # OK
                  
        if mod_result.result == 0: # OK
            mod_result.current_version = file_info.version
            self.files[file_info.filename] = \
                (file_info.version,file_info.blocklist,False)
        
        return mod_result
        

    def DeleteFile(self, file_info, context):
        del_result = SurfStoreBasic_pb2.WriteResult(result=1)
        if not self.leader:
            del_result.result = 3
            return del_result

        fn = file_info.filename

        # Does the file exist?
        if fn in self.files:
            # Version should be current_version + 1 and not already deleted
            if file_info.version == (self.files[fn][_VERS] + 1) and \
            self.files[fn][_IS_DELETED] == False:
                # Only when a request is valid (all blocks are present and the version
                # number is correct) does it need to invoke 2PC on the followers.
                ###################
                # 2PC
                if self.distributed:
                    if self.leader:
                        self.two_phase_commit("del", file_info)
                ###################
                del_result.result = 0 # OK
                self.files[fn] = (file_info.version, ['0'], True)
        
        return del_result
    
######################################################
############### Below is for part 2 ##################
######################################################

    def Vote(self, request, context):
        if not self.crashed:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=True)
        else:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=False)


    def Commit(self, request, context):
        if not self.crashed:
            log = (request.cmd, request.filename, request.version, request.blocklist)
            self.logs.append(log)
            # leader has already checked the validity of the command, so just execute it
            if request.cmd == "mod":
                self.files[request.filename] = (request.version, request.blocklist, False)
            if request.cmd == "del":
                self.files[request.filename] = (request.version, ['0'], True)
            return SurfStoreBasic_pb2.Empty()


    def Update(self, request, context):
        if self.crashed:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=False)
        # convert grpc format to python list
        leaderLogs = []
        for entry in request.allLogs:
            log = (entry.cmd, entry.filename, entry.version, entry.blocklist)
            leaderLogs.append(log)
        myLogSize, leaderLogSize = len(self.logs), len(leaderLogs)
        if myLogSize != leaderLogSize:
            for i in range(myLogSize, leaderLogSize):
                missedLog = leaderLogs[i]
                
                # update the file map
                if missedLog[0] == "mod":
                    self.files[missedLog[1]] = (missedLog[2], missedLog[3], False)
                if missedLog[0] == "del":
                    self.files[missedLog[1]] = (missedLog[2], ['0'], True)
                # append the missed log
                self.logs.append(missedLog)

        return SurfStoreBasic_pb2.SimpleAnswer(answer=True)


    def IsLeader(self, request, context):
        if self.leader == True:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=True)
        else:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=False)
    

    def Crash(self, request, context):
        # only non-leader replica can be crashed
        if self.leader == False:
            self.crashed = True
        return SurfStoreBasic_pb2.Empty()
        

    def Restore(self, request, context):
        self.crashed = False
        return SurfStoreBasic_pb2.Empty()
    

    def IsCrashed(self, request, context):
        if self.crashed == True:
            return SurfStoreBasic_pb2.SimpleAnswer(answer=True)
        else: 
            return SurfStoreBasic_pb2.SimpleAnswer(answer=False)

# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~# ~#

def parse_args():
    parser = argparse.ArgumentParser(description="MetadataStore server for SurfStore")
    parser.add_argument("config_file", type=str,
                        help="Path to configuration file")
    parser.add_argument("-n", "--number", type=int, default=1,
                        help="Set which number this server is")
    parser.add_argument("-t", "--threads", type=int, default=10,
                        help="Maximum number of concurrent threads")
    return parser.parse_args()

def serve(args, config):
    metadata_store = MetadataStore(config)

    # Who am I? Am I the leader?
    ## OUR STUFF
    leaderID = config.num_leaders
    if args.number == leaderID:
        metadata_store.leader = True # Hey, look at me, I'm the captain now.
    metadata_store.myID = args.number
    metadata_store.init_distributed_server()
    ## END

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=args.threads))
    SurfStoreBasic_pb2_grpc.add_MetadataStoreServicer_to_server(metadata_store, server)
    server.add_insecure_port("127.0.0.1:%d" % config.metadata_ports[args.number])
    print("INFO: Metadata server number %d starting" % args.number)
    server.start()
    print("Server started on 127.0.0.1:%d" % config.metadata_ports[args.number])

    try:
        while True:
            if metadata_store.distributed:
                if metadata_store.leader:
                    metadata_store.update_crashed_server()
            time.sleep(0.5)
            pass

    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    args = parse_args()
    config = SurfStoreConfigReader(args.config_file)

    if args.number > config.num_metadata_servers:
        raise RuntimeError("metadata%d not defined in config file" % args.number)

    serve(args, config)
