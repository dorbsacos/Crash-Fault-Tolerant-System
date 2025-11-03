import socket
import threading
import time
import queue
import random
from typing import Dict, List, Any, Tuple, Optional, Set
import json

import config
from message import (
    MessageType, create_prepare, create_promise, create_accept,
    create_accepted, create_commit, create_new_view, create_reply,
    create_heartbeat, serialize_message, deserialize_message, validate_message
)
from database import Database
from log import RequestLog, Status
from utils import (
    is_ballot_greater, is_ballot_greater_or_equal, create_ballot,
    merge_accept_logs, fill_gaps_with_noops, get_current_timestamp,
    has_timeout_expired, get_highest_sequence_number
)


class PaxosNode:
    """Paxos node implementing Stable-Leader Paxos protocol."""
    
    def __init__(self, node_id: int, control_queue: Optional[queue.Queue] = None, response_queue: Optional[queue.Queue] = None):
        self.node_id = node_id
        self.host, self.port = config.NODE_ADDRESSES[node_id]
        self.response_queue = response_queue
        
        self.is_leader = False
        self.current_leader_id = None
        self.current_ballot = create_ballot(0, node_id)
        self.highest_ballot_seen = create_ballot(0, 0)
        
        self.next_sequence_number = 1
        
        self.database = Database()
        self.log = RequestLog(node_id)
        
        self.promise_count = 0
        self.promise_messages: List[Dict[str, Any]] = []
        self.last_prepare_time = 0
        
        self.accepted_count: Dict[int, int] = {}
        self.accepted_nodes: Dict[int, Set[int]] = {}
        self.committed_sequences: Set[int] = set()
        
        self.leader_timeout_ms = random.randint(
            config.LEADER_TIMEOUT_MIN_MS, 
            config.LEADER_TIMEOUT_MAX_MS
        )
        self.last_leader_message_time = get_current_timestamp()
        self.last_heartbeat_time = get_current_timestamp()
        self.timer_active = True
        
        self.election_in_progress = False
        self.suppress_elections_until = 0.0
        self.election_start_time = 0.0
        
        self.is_disconnected = False
        self.control_queue = control_queue if control_queue else queue.Queue()
        
        self.server_socket = None
        self.running = False
        
        self.client_last_reply: Dict[str, Tuple[float, str]] = {}
        self.request_timestamps: Dict[int, Tuple[str, float]] = {}
        self.replied_sequences: Set[int] = set()
        self.client_request_to_seq: Dict[Tuple[str, float], int] = {}
        
        self.new_view_messages: List[Dict[str, Any]] = []
        self.pending_requests: queue.Queue = queue.Queue()
        
    
    def start(self):
        """Start node: network, timers, message processing."""
        self.running = True
        
        self._start_server()
        
        timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        timer_thread.start()
        
        msg_thread = threading.Thread(target=self._process_messages, daemon=True)
        msg_thread.start()
        
        control_thread = threading.Thread(target=self._process_control_commands, daemon=True)
        control_thread.start()
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Started on {self.host}:{self.port} (timeout={self.leader_timeout_ms}ms)")
        else:
            print(f"[Node {self.node_id}] Started on {self.host}:{self.port}")
    
    
    def _start_server(self):
        """Start TCP server to listen for incoming messages."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.server_socket.settimeout(config.SOCKET_TIMEOUT_SEC)
        
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
    
    
    def _accept_connections(self):
        """Accept incoming connections and handle them."""
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                handler = threading.Thread(
                    target=self._handle_connection,
                    args=(client_sock,),
                    daemon=True
                )
                handler.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[Node {self.node_id}] Accept error: {e}")
    
    
    def _handle_connection(self, client_sock: socket.socket):
        """Handle single connection: receive and process message."""
        try:
            data = client_sock.recv(config.MAX_MESSAGE_SIZE)
            if not data:
                return
            
            message = deserialize_message(data)
            
            if validate_message(message):
                self._handle_message(message)
            else:
                print(f"[Node {self.node_id}] Invalid message: {message}")
                
        except Exception as e:
            print(f"[Node {self.node_id}] Connection error: {e}")
        finally:
            client_sock.close()
    
    
    def _send_message(self, message: Dict[str, Any], target_node_id: int):
        """Send message to another node. Blocked when disconnected."""
        if self.is_disconnected:
            return
        
        try:
            target_host, target_port = config.NODE_ADDRESSES[target_node_id]
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((target_host, target_port))
            sock.sendall(serialize_message(message))
            sock.close()
            
        except Exception as e:
            if config.DEBUG_MODE:
                print(f"[Node {self.node_id}] Failed to send to Node {target_node_id}: {e}")
    
    
    def _broadcast_message(self, message: Dict[str, Any], exclude_self: bool = True):
        """Broadcast message to all nodes."""
        for node_id in config.NODE_IDS:
            if exclude_self and node_id == self.node_id:
                continue
            self._send_message(message, node_id)
    
    
    def _timer_loop(self):
        """Timer loop for leader timeout and heartbeats."""
        while self.running:
            time.sleep(0.1)
            
            if not self.timer_active or self.is_disconnected:
                continue
            
            if self.is_leader:
                if has_timeout_expired(self.last_heartbeat_time, config.HEARTBEAT_INTERVAL_MS):
                    self._send_heartbeat()
            else:
                if has_timeout_expired(self.last_leader_message_time, self.leader_timeout_ms):
                    now = get_current_timestamp()
                    
                    if self.election_in_progress:
                        if now - self.election_start_time > config.ELECTION_TIMEOUT_SEC:
                            if config.VERBOSE_LOGGING:
                                print(f"[Node {self.node_id}] Election timed out, will retry")
                            self.election_in_progress = False
                        else:
                            continue
                    
                    if now < self.suppress_elections_until:
                        if config.VERBOSE_LOGGING:
                            print(f"[Node {self.node_id}] Election suppressed (quiet period)")
                        continue
                    
                    if now - self.last_leader_message_time < config.LEADER_FRESH_THRESHOLD_SEC:
                        if config.VERBOSE_LOGGING:
                            print(f"[Node {self.node_id}] Election skipped (leader fresh)")
                        continue
                    
                    if not has_timeout_expired(self.last_prepare_time, config.PREPARE_INTERVAL_MS):
                        continue
                    
                    self._initiate_leader_election()
    
    
    def _initiate_leader_election(self):
        """Initiate leader election by sending PREPARE."""
        if self.is_disconnected:
            return
        
        self.election_in_progress = True
        self.election_start_time = get_current_timestamp()
        
        round_num = self.highest_ballot_seen[0] + 1
        self.current_ballot = create_ballot(round_num, self.node_id)
        
        self.promise_count = 0
        self.promise_messages = []
        
        prepare_msg = create_prepare(self.current_ballot)
        self._broadcast_message(prepare_msg, exclude_self=False)
        
        self.log.log_message(prepare_msg)
        
        self.last_prepare_time = get_current_timestamp()
        
        self.leader_timeout_ms = random.randint(
            config.LEADER_TIMEOUT_MIN_MS, 
            config.LEADER_TIMEOUT_MAX_MS
        )
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Initiated leader election with ballot {self.current_ballot} (timeout={self.leader_timeout_ms}ms)")
    
    
    def _handle_message(self, message: Dict[str, Any]):
        """Route message to appropriate handler. Ignore when disconnected."""
        if self.is_disconnected:
            return
        
        msg_type = message.get("type")
        
        if msg_type != MessageType.HEARTBEAT:
            self.log.log_message(message)
        
        if msg_type in [MessageType.ACCEPT, MessageType.COMMIT, MessageType.NEW_VIEW, MessageType.HEARTBEAT]:
            self.last_leader_message_time = get_current_timestamp()
        
        if msg_type == MessageType.PREPARE:
            self._handle_prepare(message)
        elif msg_type == MessageType.PROMISE:
            self._handle_promise(message)
        elif msg_type == MessageType.ACCEPT:
            self._handle_accept(message)
        elif msg_type == MessageType.ACCEPTED:
            self._handle_accepted(message)
        elif msg_type == MessageType.COMMIT:
            self._handle_commit(message)
        elif msg_type == MessageType.NEW_VIEW:
            self._handle_new_view(message)
        elif msg_type == MessageType.HEARTBEAT:
            self._handle_heartbeat(message)
        elif msg_type == MessageType.REQUEST:
            self._handle_request(message)
        else:
            print(f"[Node {self.node_id}] Unknown message type: {msg_type}")
    
    
    def _handle_prepare(self, message: Dict[str, Any]):
        """Handle PREPARE: send PROMISE if ballot is higher."""
        ballot = tuple(message["ballot"])
        
        if is_ballot_greater(ballot, self.highest_ballot_seen):
            self.highest_ballot_seen = ballot
            self.suppress_elections_until = get_current_timestamp() + config.QUIET_PERIOD_SEC
            
            accept_log = self.log.get_accept_log()
            promise_msg = create_promise(ballot, accept_log)
            
            proposer_node_id = ballot[1]
            self._send_message(promise_msg, proposer_node_id)
            
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Sent PROMISE for ballot {ballot}")
        else:
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Rejected PREPARE {ballot}, highest seen: {self.highest_ballot_seen}")
    
    
    def _handle_promise(self, message: Dict[str, Any]):
        """Handle PROMISE: become leader when quorum reached."""
        ballot = tuple(message["ballot"])
        
        if ballot != self.current_ballot:
            return
        
        if self.is_leader and self.current_ballot == ballot:
            return
        
        self.promise_messages.append(message)
        self.promise_count += 1
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Received PROMISE ({self.promise_count}/{config.QUORUM_SIZE})")
        
        if self.promise_count == config.QUORUM_SIZE:
            self.election_in_progress = False
            self._become_leader()
    
    
    def _become_leader(self):
        """Become leader: merge logs, send NEW-VIEW."""
        self.is_leader = True
        self.current_leader_id = self.node_id
        self.suppress_elections_until = get_current_timestamp() + config.QUIET_PERIOD_SEC
        
        all_accept_logs = [msg["accept_log"] for msg in self.promise_messages]
        leader_accept_log = self.log.get_accept_log()
        all_accept_logs.append(leader_accept_log)
        
        merged_log = merge_accept_logs(all_accept_logs)
        
        if merged_log:
            complete_log = fill_gaps_with_noops(merged_log, self.current_ballot)
        else:
            complete_log = []
        
        new_view_msg = create_new_view(self.current_ballot, complete_log)
        self._broadcast_message(new_view_msg, exclude_self=False)
        
        self.new_view_messages.append(new_view_msg)
        self._handle_new_view(new_view_msg)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Became LEADER with ballot {self.current_ballot}")
        
        self.promise_count = 0
        self.promise_messages = []
    
    
    def _handle_new_view(self, message: Dict[str, Any]):
        """Handle NEW-VIEW: synchronize with new leader's view."""
        ballot = tuple(message["ballot"])
        accept_log = message["accept_log"]
        
        self.current_leader_id = ballot[1]
        self.highest_ballot_seen = ballot
        self.current_ballot = ballot
        
        if self.current_leader_id != self.node_id:
            self.is_leader = False
        
        self.suppress_elections_until = get_current_timestamp() + config.QUIET_PERIOD_SEC
        
        if message not in self.new_view_messages:
            self.new_view_messages.append(message)
        
        for entry in accept_log:
            _, seq_num, transaction = entry
            
            self.log.add_entry(seq_num, ballot, transaction)
            
            if self.is_leader and transaction != "no-op":
                if isinstance(transaction, dict) and "client_id" in transaction:
                    client_id = transaction["client_id"]
                    timestamp = transaction["timestamp"]
                    request_key = (client_id, timestamp)
                    self.client_request_to_seq[request_key] = seq_num
            
            accepted_msg = create_accepted(ballot, seq_num, transaction, self.node_id)
            self._send_message(accepted_msg, self.current_leader_id)
        
        if accept_log:
            highest_seq = get_highest_sequence_number(accept_log)
            self.next_sequence_number = max(self.next_sequence_number, highest_seq + 1)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Processed NEW-VIEW from Node {self.current_leader_id}")
    
    
    def _handle_accept(self, message: Dict[str, Any]):
        """Handle ACCEPT: add to log and send ACCEPTED."""
        ballot = tuple(message["ballot"])
        sequence_number = message["sequence_number"]
        request = message["request"]
        
        if is_ballot_greater_or_equal(ballot, self.highest_ballot_seen):
            if is_ballot_greater(ballot, self.highest_ballot_seen):
                self.suppress_elections_until = get_current_timestamp() + config.QUIET_PERIOD_SEC
            
            self.highest_ballot_seen = ballot
            self.current_ballot = ballot
            
            self.log.add_entry(sequence_number, ballot, request)
            self.log.mark_accepted(sequence_number)
            
            accepted_msg = create_accepted(ballot, sequence_number, request, self.node_id)
            leader_id = ballot[1]
            self._send_message(accepted_msg, leader_id)
            
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Accepted seq={sequence_number}")
    
    
    def _handle_accepted(self, message: Dict[str, Any]):
        """Handle ACCEPTED: commit when quorum reached (leader only)."""
        if not self.is_leader:
            return
        
        ballot = tuple(message["ballot"])
        sequence_number = message["sequence_number"]
        node_id = message["node_id"]
        request = message["request"]
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Received ACCEPTED for seq={sequence_number} from Node {node_id}, ballot={ballot}, current_ballot={self.current_ballot}")
        
        if ballot != self.current_ballot:
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Rejected ACCEPTED (ballot mismatch: {ballot} != {self.current_ballot})")
            return
        
        if sequence_number not in self.accepted_count:
            self.accepted_count[sequence_number] = 0
            self.accepted_nodes[sequence_number] = set()
        
        if node_id not in self.accepted_nodes[sequence_number]:
            self.accepted_count[sequence_number] += 1
            self.accepted_nodes[sequence_number].add(node_id)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Count for seq={sequence_number}: {self.accepted_count[sequence_number]}/{config.QUORUM_SIZE}")
        
        if (self.accepted_count[sequence_number] >= config.QUORUM_SIZE and 
            sequence_number not in self.committed_sequences):
            
            self.committed_sequences.add(sequence_number)
            
            commit_msg = create_commit(ballot, sequence_number, request)
            self._broadcast_message(commit_msg, exclude_self=True)
            self._handle_commit(commit_msg)
            
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Committed seq={sequence_number}")
    
    
    def _handle_commit(self, message: Dict[str, Any]):
        """Handle COMMIT: mark committed and try to execute."""
        ballot = tuple(message["ballot"])
        sequence_number = message["sequence_number"]
        request = message["request"]
        
        if is_ballot_greater_or_equal(ballot, self.highest_ballot_seen):
            if is_ballot_greater(ballot, self.highest_ballot_seen):
                self.suppress_elections_until = get_current_timestamp() + config.QUIET_PERIOD_SEC
            
            self.highest_ballot_seen = ballot
            self.current_ballot = ballot
        
        if self.log.has_sequence(sequence_number):
            self.log.mark_committed(sequence_number)
        else:
            self.log.add_entry(sequence_number, ballot, request)
            self.log.mark_committed(sequence_number)
        
        self._try_execute(sequence_number)
        
        if (self.is_leader and 
            sequence_number not in self.replied_sequences and
            self.log.get_status(sequence_number) == Status.COMMITTED):
            
            entry = self.log.get_entry(sequence_number)
            if entry and entry.transaction != "no-op" and isinstance(entry.transaction, dict):
                self._send_commit_reply(sequence_number, entry.transaction)
                self.replied_sequences.add(sequence_number)
    
    
    def _try_execute(self, sequence_number: int):
        """Try to execute sequence if all lower sequences are executed."""
        if not self.log.can_execute_sequence(sequence_number):
            if config.VERBOSE_LOGGING:
                for i in range(1, sequence_number):
                    status = self.log.get_status(i)
                    if status != Status.EXECUTED:
                        print(f"[Node {self.node_id}] Cannot execute seq={sequence_number}: seq={i} not executed (status={status})")
                        break
            return
        
        if self.log.get_status(sequence_number) == Status.EXECUTED:
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Seq={sequence_number} already executed, skipping")
            return
        
        status = self.log.get_status(sequence_number)
        if status != Status.COMMITTED:
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Cannot execute seq={sequence_number}: not committed (status={status})")
            return
        
        entry = self.log.get_entry(sequence_number)
        if not entry:
            return
        
        if entry.transaction == "no-op":
            transaction_to_execute = "no-op"
        elif isinstance(entry.transaction, dict) and "transaction" in entry.transaction:
            transaction_to_execute = tuple(entry.transaction["transaction"])
        else:
            transaction_to_execute = entry.transaction
        
        result = self.database.execute_transaction(transaction_to_execute)
        self.log.mark_executed(sequence_number)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Executed seq={sequence_number}, result={result}")
        
        if self.is_leader and entry.transaction != "no-op":
            self._send_client_reply(sequence_number, entry.transaction, result)
        
        self._try_execute(sequence_number + 1)
    
    
    def _handle_request(self, message: Dict[str, Any]):
        """Handle client REQUEST: assign sequence and propose."""
        transaction = tuple(message["transaction"])
        timestamp = message["timestamp"]
        client_id = message["client_id"]
        request_key = (client_id, timestamp)
        
        if client_id in self.client_last_reply:
            last_timestamp, last_result = self.client_last_reply[client_id]
            if timestamp <= last_timestamp:
                reply_msg = create_reply(self.current_ballot, timestamp, client_id, last_result)
                self._send_reply_to_client(client_id, reply_msg)
                
                if config.VERBOSE_LOGGING:
                    print(f"[Node {self.node_id}] Duplicate request from {client_id}, resent reply")
                return
        
        if not self.is_leader:
            if self.current_leader_id:
                self._send_message(message, self.current_leader_id)
            else:
                self.pending_requests.put(message)
            return
        
        if request_key in self.client_request_to_seq:
            if config.VERBOSE_LOGGING:
                seq_num = self.client_request_to_seq[request_key]
                print(f"[Node {self.node_id}] Duplicate request from {client_id}, already assigned seq={seq_num}")
            return
        
        seq_num = self.next_sequence_number
        self.next_sequence_number += 1
        
        self.client_request_to_seq[request_key] = seq_num
        self.request_timestamps[seq_num] = (client_id, timestamp)
        
        self.log.add_entry(seq_num, self.current_ballot, message)
        
        self.accepted_count[seq_num] = 1
        self.accepted_nodes[seq_num] = {self.node_id}
        self.log.mark_accepted(seq_num)
        
        accept_msg = create_accept(self.current_ballot, seq_num, message)
        self._broadcast_message(accept_msg, exclude_self=False)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Proposed seq={seq_num} for client {client_id}")
    
    
    def _send_heartbeat(self):
        """Send heartbeat to all backups (leader only)."""
        if not self.is_leader or self.is_disconnected:
            return
        
        heartbeat_msg = create_heartbeat(self.current_ballot, self.node_id)
        self._broadcast_message(heartbeat_msg, exclude_self=True)
        
        self.last_heartbeat_time = get_current_timestamp()
    
    
    def _handle_heartbeat(self, message: Dict[str, Any]):
        """Handle HEARTBEAT: update leader info."""
        ballot = tuple(message["ballot"])
        leader_id = message["leader_id"]
        
        if is_ballot_greater_or_equal(ballot, self.highest_ballot_seen):
            self.highest_ballot_seen = ballot
            self.current_ballot = ballot
            self.current_leader_id = leader_id
    
    
    def _send_reply_to_client(self, client_id: str, reply_msg: Dict[str, Any]):
        """Send REPLY message to client."""
        if self.is_disconnected:
            return
        
        try:
            client_host, client_port = config.CLIENT_ADDRESSES[client_id]
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((client_host, client_port))
            sock.sendall(serialize_message(reply_msg))
            sock.close()
            
            if config.VERBOSE_LOGGING:
                result = reply_msg.get("result", "unknown")
                print(f"[Node {self.node_id}] Sent REPLY to {client_id}: {result}")
                
        except Exception as e:
            if config.DEBUG_MODE:
                print(f"[Node {self.node_id}] Failed to send reply to {client_id}: {e}")
    
    
    def _send_commit_reply(self, sequence_number: int, transaction: Any):
        """Send immediate reply when transaction is committed but not yet executed."""
        if not isinstance(transaction, dict):
            return
        
        client_id = transaction.get("client_id")
        timestamp = transaction.get("timestamp")
        
        if not client_id or timestamp is None:
            return
        
        result = "committed"
        reply_msg = create_reply(self.current_ballot, timestamp, client_id, result)
        
        self.client_last_reply[client_id] = (timestamp, result)
        self._send_reply_to_client(client_id, reply_msg)
        
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Sent COMMIT REPLY to {client_id}: {result} (execution pending)")
    
    
    def _send_client_reply(self, sequence_number: int, transaction: Any, result: str):
        """Send reply to client after execution."""
        if transaction == "no-op":
            return
        
        if sequence_number in self.replied_sequences:
            if config.VERBOSE_LOGGING:
                print(f"[Node {self.node_id}] Skipping duplicate reply for seq={sequence_number} (already sent)")
            return
        
        if not isinstance(transaction, dict) or "client_id" not in transaction:
            print(f"[Node {self.node_id}] ERROR: Invalid transaction format for reply: {type(transaction)}")
            return
        
        client_id = transaction["client_id"]
        timestamp = transaction["timestamp"]
        
        self.client_last_reply[client_id] = (timestamp, result)
        
        reply_msg = create_reply(self.current_ballot, timestamp, client_id, result)
        self._send_reply_to_client(client_id, reply_msg)
        
        self.replied_sequences.add(sequence_number)
    
    
    def _process_messages(self):
        """Background thread to process pending requests."""
        while self.running:
            time.sleep(0.1)
            
            if self.is_leader:
                while not self.pending_requests.empty():
                    try:
                        request = self.pending_requests.get_nowait()
                        self._handle_request(request)
                    except queue.Empty:
                        break
    
    
    def _process_control_commands(self):
        """Background thread to process control commands from test runner."""
        while self.running:
            try:
                command = self.control_queue.get(timeout=0.1)
                
                cmd_type = command.get("command")
                
                if cmd_type == "disconnect":
                    self.disconnect()
                elif cmd_type == "reconnect":
                    self.reconnect()
                elif cmd_type == "pause_timer":
                    self.pause_timer()
                elif cmd_type == "resume_timer":
                    self.resume_timer()
                elif cmd_type == "print_db":
                    self.print_database()
                elif cmd_type == "print_log":
                    self.print_log()
                elif cmd_type == "print_status":
                    seq_num = command.get("sequence_number", 1)
                    self.print_status(seq_num)
                elif cmd_type == "print_view":
                    self.print_view()
                elif cmd_type == "query_leader":
                    if self.response_queue:
                        self.response_queue.put({
                            "node_id": self.node_id,
                            "is_leader": self.is_leader,
                            "ballot": self.current_ballot
                        })
                else:
                    if config.DEBUG_MODE:
                        print(f"[Node {self.node_id}] Unknown control command: {cmd_type}")
                    
            except queue.Empty:
                continue
            except Exception as e:
                if config.DEBUG_MODE:
                    print(f"[Node {self.node_id}] Error processing control command: {e}")
    
    
    def disconnect(self):
        """Simulate soft failure: stop sending/receiving messages."""
        self.is_disconnected = True
        self.timer_active = False
        print(f"[Node {self.node_id}] DISCONNECTED (soft failure - LF)")
    
    
    def reconnect(self):
        """Reconnect after soft failure."""
        self.is_disconnected = False
        self.timer_active = True
        
        grace_period_ms = config.HEARTBEAT_INTERVAL_MS * 3
        self.last_leader_message_time = get_current_timestamp() + (grace_period_ms / 1000.0)
        
        self.is_leader = False
        self.current_leader_id = None
        print(f"[Node {self.node_id}] RECONNECTED")
    
    
    def pause_timer(self):
        """Pause timer for result presentation."""
        self.timer_active = False
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Timer PAUSED")
    
    
    def resume_timer(self):
        """Resume timer."""
        self.timer_active = True
        self.last_leader_message_time = get_current_timestamp()
        if config.VERBOSE_LOGGING:
            print(f"[Node {self.node_id}] Timer RESUMED")
    
    
    def print_log(self):
        """Print log entries and messages."""
        self.log.print_log()
    
    
    def print_status(self, sequence_number: int):
        """Print status for sequence."""
        self.log.print_status(sequence_number)
    
    
    def print_database(self):
        """Print database state."""
        self.database.print_database()
    
    
    def print_view(self):
        """Print all NEW-VIEW messages."""
        print("\n" + "="*60)
        print(f"NEW-VIEW MESSAGES FOR NODE {self.node_id}")
        print("="*60)
        
        if not self.new_view_messages:
            print("No NEW-VIEW messages received.")
        else:
            for i, msg in enumerate(self.new_view_messages, 1):
                ballot = msg["ballot"]
                accept_log_size = len(msg["accept_log"])
                print(f"{i}. Ballot={ballot}, AcceptLog entries={accept_log_size}")
        
        print("="*60 + "\n")
    
    
    def stop(self):
        """Stop the node."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print(f"[Node {self.node_id}] Stopped")
