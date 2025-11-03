# System configuration

NUM_NODES = 5
FAULT_TOLERANCE = 2
QUORUM_SIZE = FAULT_TOLERANCE + 1

NODE_ADDRESSES = {
    1: ("localhost", 5001),
    2: ("localhost", 5002),
    3: ("localhost", 5003),
    4: ("localhost", 5004),
    5: ("localhost", 5005),
}

NODE_IDS = list(NODE_ADDRESSES.keys())


NUM_CLIENTS = 10
CLIENT_IDS = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]

CLIENT_ADDRESSES = {
    "A": ("localhost", 6001),
    "B": ("localhost", 6002),
    "C": ("localhost", 6003),
    "D": ("localhost", 6004),
    "E": ("localhost", 6005),
    "F": ("localhost", 6006),
    "G": ("localhost", 6007),
    "H": ("localhost", 6008),
    "I": ("localhost", 6009),
    "J": ("localhost", 6010),
}

INITIAL_BALANCE = 10


LEADER_TIMEOUT_MIN_MS = 1000
LEADER_TIMEOUT_MAX_MS = 2000

PREPARE_INTERVAL_MS = 500
HEARTBEAT_INTERVAL_MS = 200

ELECTION_TIMEOUT_SEC = 2.0
QUIET_PERIOD_SEC = 0.6
LEADER_FRESH_THRESHOLD_SEC = 1.0

CLIENT_TIMEOUT_MS = 10000
SOCKET_TIMEOUT_SEC = 0.1


MAX_MESSAGE_SIZE = 65536


ENABLE_CHECKPOINTING = False
CHECKPOINT_INTERVAL = 100


VERBOSE_LOGGING = False
DEBUG_MODE = False
