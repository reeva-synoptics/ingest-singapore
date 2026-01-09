
# 10.0.0.114 dev address
# mesonet-v2.entry.int.synopticdata.net prod address

import os


class Args:
    def __init__(self):
        self.dev = os.getenv('DEV', 'false').lower() in ['true', '1', 't']
        self.local_run = os.getenv('LOCAL_RUN', 'false').lower() in ['true', '1', 't']
        self.log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.poe_socket_address = os.getenv('POE_SOCKET_ADDRESS', "10.0.0.114")
        self.poe_socket_port = int(os.getenv('POE_SOCKET_PORT', 8095))
        self.poe_chunk_size = int(os.getenv('POE_CHUNK_SIZE', 2000))
        self.force_io_dump = os.getenv('FORCE_IO_DUMP', 'false').lower() in ['true', '1', 't']
        self.metamgr_socket_address = os.getenv('METAMGR_SOCKET_ADDRESS', "10.14.159.245")
        self.metamgr_socket_port = os.getenv('METAMGR_SOCKET_PORT', 8888)
# Create an instance of Args to be imported into the lambda
args = Args()