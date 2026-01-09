# 10.0.0.114 dev address
# mesonet-v2.entry.int.synopticdata.net prod address

import os

class Args:
    def __init__(self):
        self.dev = os.getenv('DEV', 'false').lower() in ['true', '1', 't']
        self.local_run = os.getenv('LOCAL_RUN', 'false').lower() in ['true', '1', 't']
        self.log_level = os.getenv('LOG_LEVEL', 'WARNING').upper()
        self.metamgr_socket_address = os.getenv('METAMGR_SOCKET_ADDRESS', "10.14.159.245")
        self.metamgr_socket_port = int(os.getenv('METAMGR_SOCKET_PORT', 8888))
        self.endpoint = os.getenv('ENDPOINT', 'stations')
        self.first_run = os.getenv('FIRST_RUN', 'false').lower() in ['true', '1', 't']
        self.force_io_dump = os.getenv('FORCE_IO_DUMP', 'false').lower() in ['true', '1', 't']

# Create an instance of Args to be imported into the lambda
args = Args()