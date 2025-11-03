import json
from typing import Any, Dict, List, Tuple, Optional


class MessageType:
    PREPARE = "PREPARE"
    PROMISE = "PROMISE"
    ACCEPT = "ACCEPT"
    ACCEPTED = "ACCEPTED"
    COMMIT = "COMMIT"
    NEW_VIEW = "NEW_VIEW"
    REQUEST = "REQUEST"
    REPLY = "REPLY"
    HEARTBEAT = "HEARTBEAT"


def create_prepare(ballot: Tuple[int, int]) -> Dict[str, Any]:
    """Create PREPARE message for leader election."""
    return {
        "type": MessageType.PREPARE,
        "ballot": ballot
    }


def create_promise(ballot: Tuple[int, int], accept_log: List[Tuple[Tuple[int, int], int, Any]]) -> Dict[str, Any]:
    """Create PROMISE message with accept_log of (ballot, sequence_number, transaction) tuples."""
    return {
        "type": MessageType.PROMISE,
        "ballot": ballot,
        "accept_log": accept_log
    }


def create_accept(ballot: Tuple[int, int], sequence_number: int, request: Dict[str, Any]) -> Dict[str, Any]:
    """Create ACCEPT message for transaction proposal."""
    return {
        "type": MessageType.ACCEPT,
        "ballot": ballot,
        "sequence_number": sequence_number,
        "request": request
    }


def create_accepted(ballot: Tuple[int, int], sequence_number: int, request: Dict[str, Any], node_id: int) -> Dict[str, Any]:
    """Create ACCEPTED message in response to ACCEPT."""
    return {
        "type": MessageType.ACCEPTED,
        "ballot": ballot,
        "sequence_number": sequence_number,
        "request": request,
        "node_id": node_id
    }


def create_commit(ballot: Tuple[int, int], sequence_number: int, request: Dict[str, Any]) -> Dict[str, Any]:
    """Create COMMIT message to finalize request."""
    return {
        "type": MessageType.COMMIT,
        "ballot": ballot,
        "sequence_number": sequence_number,
        "request": request
    }


def create_new_view(ballot: Tuple[int, int], accept_log: List[Tuple[Tuple[int, int], int, Any]]) -> Dict[str, Any]:
    """Create NEW-VIEW message after leader election with aggregated accept_log."""
    return {
        "type": MessageType.NEW_VIEW,
        "ballot": ballot,
        "accept_log": accept_log
    }


def create_request(transaction: Tuple[str, str, int], timestamp: float, client_id: str) -> Dict[str, Any]:
    """Create REQUEST message from client."""
    return {
        "type": MessageType.REQUEST,
        "transaction": transaction,
        "timestamp": timestamp,
        "client_id": client_id
    }


def create_reply(ballot: Tuple[int, int], timestamp: float, client_id: str, result: str) -> Dict[str, Any]:
    """Create REPLY message to client with result ('success' or 'failed')."""
    return {
        "type": MessageType.REPLY,
        "ballot": ballot,
        "timestamp": timestamp,
        "client_id": client_id,
        "result": result
    }


def create_heartbeat(ballot: Tuple[int, int], leader_id: int) -> Dict[str, Any]:
    """Create HEARTBEAT message from leader to prevent backup timeouts."""
    return {
        "type": MessageType.HEARTBEAT,
        "ballot": ballot,
        "leader_id": leader_id
    }


def serialize_message(message: Dict[str, Any]) -> bytes:
    """Serialize message dict to JSON bytes."""
    json_str = json.dumps(message)
    return json_str.encode('utf-8')


def deserialize_message(data: bytes) -> Dict[str, Any]:
    """Deserialize JSON bytes to message dict."""
    json_str = data.decode('utf-8')
    return json.loads(json_str)


def validate_message(message: Dict[str, Any]) -> bool:
    """Validate message has required fields for its type."""
    if "type" not in message:
        return False
    
    msg_type = message["type"]
    
    required_fields = {
        MessageType.PREPARE: ["ballot"],
        MessageType.PROMISE: ["ballot", "accept_log"],
        MessageType.ACCEPT: ["ballot", "sequence_number", "request"],
        MessageType.ACCEPTED: ["ballot", "sequence_number", "request", "node_id"],
        MessageType.COMMIT: ["ballot", "sequence_number", "request"],
        MessageType.NEW_VIEW: ["ballot", "accept_log"],
        MessageType.REQUEST: ["transaction", "timestamp", "client_id"],
        MessageType.REPLY: ["ballot", "timestamp", "client_id", "result"],
        MessageType.HEARTBEAT: ["ballot", "leader_id"],
    }
    
    if msg_type not in required_fields:
        return False
    
    for field in required_fields[msg_type]:
        if field not in message:
            return False
    
    return True


def get_message_type(message: Dict[str, Any]) -> Optional[str]:
    """Get message type."""
    return message.get("type")


def is_protocol_message(message: Dict[str, Any]) -> bool:
    """Check if message is Paxos protocol message (not from client)."""
    msg_type = get_message_type(message)
    return msg_type in [
        MessageType.PREPARE,
        MessageType.PROMISE,
        MessageType.ACCEPT,
        MessageType.ACCEPTED,
        MessageType.COMMIT,
        MessageType.NEW_VIEW
    ]


def is_client_message(message: Dict[str, Any]) -> bool:
    """Check if message is from/to client."""
    msg_type = get_message_type(message)
    return msg_type in [MessageType.REQUEST, MessageType.REPLY]
