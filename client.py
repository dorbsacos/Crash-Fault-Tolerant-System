import socket
import threading
import time
import queue
from typing import Dict, Optional, Tuple, List

import config
from message import (
    MessageType, create_request, serialize_message, 
    deserialize_message, validate_message
)
from utils import get_current_timestamp


class Client:
    """Single client with independent state and timer."""
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.host, self.port = config.CLIENT_ADDRESSES[client_id]
        
        self.current_leader_id = 1
        self.last_timestamp = 0
        self.last_reply = None
        self.pending_request = None
        self.pending_timestamp = None
        
        self.reply_queue: queue.Queue = queue.Queue()
        
        self.server_socket = None
        self.running = False
        
        self._start_listener()
    
    
    def _start_listener(self):
        """Start listening for REPLY messages from nodes."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        self.server_socket.settimeout(config.SOCKET_TIMEOUT_SEC)
        
        listener_thread = threading.Thread(target=self._accept_replies, daemon=True)
        listener_thread.start()
        
        if config.VERBOSE_LOGGING:
            print(f"[Client {self.client_id}] Listening on {self.host}:{self.port}")
    
    
    def _accept_replies(self):
        """Accept incoming REPLY messages."""
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                
                handler = threading.Thread(
                    target=self._handle_reply_connection,
                    args=(conn,),
                    daemon=True
                )
                handler.start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[Client {self.client_id}] Accept error: {e}")
    
    
    def _handle_reply_connection(self, conn: socket.socket):
        """Handle a single REPLY connection."""
        try:
            data = conn.recv(config.MAX_MESSAGE_SIZE)
            if not data:
                return
            
            message = deserialize_message(data)
            
            if validate_message(message) and message.get("type") == MessageType.REPLY:
                reply_timestamp = message.get("timestamp")
                
                if self.pending_timestamp is not None and reply_timestamp == self.pending_timestamp:
                    self.reply_queue.put(message)
                    
                    if config.VERBOSE_LOGGING:
                        result = message.get("result", "unknown")
                        print(f"[Client {self.client_id}] Received REPLY: {result}")
                elif config.DEBUG_MODE:
                    print(f"[Client {self.client_id}] Ignoring reply with mismatched timestamp")
            
        except Exception as e:
            print(f"[Client {self.client_id}] Reply handling error: {e}")
        finally:
            conn.close()
    
    
    def send_transaction(self, transaction: Tuple[str, str, int]) -> Optional[str]:
        """Send transaction to leader, broadcast on timeout."""
        timestamp = get_current_timestamp()
        self.last_timestamp = timestamp
        self.pending_timestamp = timestamp
        
        request_msg = create_request(transaction, timestamp, self.client_id)
        
        try:
            reply = self._send_to_node(request_msg, self.current_leader_id)
            
            if reply:
                return self._process_reply(reply)
            
            if config.VERBOSE_LOGGING:
                print(f"[Client {self.client_id}] Timeout, broadcasting to all nodes...")
            
            reply = self._broadcast_request(request_msg)
            
            if reply:
                return self._process_reply(reply)
            
            print(f"[Client {self.client_id}] All retry attempts failed for {transaction}")
            return None
            
        finally:
            self.pending_timestamp = None
    
    
    def _send_to_node(self, request_msg: Dict, target_node_id: int) -> Optional[Dict]:
        """Send REQUEST to specific node, wait for REPLY."""
        try:
            node_host, node_port = config.NODE_ADDRESSES[target_node_id]
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((node_host, node_port))
            sock.sendall(serialize_message(request_msg))
            sock.close()
            
            if config.VERBOSE_LOGGING:
                print(f"[Client {self.client_id}] Sent REQUEST to Node {target_node_id}")
            
            try:
                reply = self.reply_queue.get(timeout=config.CLIENT_TIMEOUT_MS / 1000)
                return reply
            except queue.Empty:
                return None
                
        except ConnectionRefusedError:
            if config.DEBUG_MODE:
                print(f"[Client {self.client_id}] Node {target_node_id} unreachable")
            return None
        except Exception as e:
            if config.DEBUG_MODE:
                print(f"[Client {self.client_id}] Error sending to Node {target_node_id}: {e}")
            return None
    
    
    def _broadcast_request(self, request_msg: Dict) -> Optional[Dict]:
        """Broadcast REQUEST to all nodes, return first valid reply."""
        for node_id in config.NODE_IDS:
            try:
                node_host, node_port = config.NODE_ADDRESSES[node_id]
                
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((node_host, node_port))
                sock.sendall(serialize_message(request_msg))
                sock.close()
                
            except:
                continue
        
        try:
            reply = self.reply_queue.get(timeout=config.CLIENT_TIMEOUT_MS / 1000)
            return reply
        except queue.Empty:
            return None
    
    
    def _process_reply(self, reply: Dict) -> str:
        """Process REPLY, update leader based on ballot."""
        ballot = reply.get("ballot", (0, 1))
        self.current_leader_id = ballot[1]
        
        self.last_reply = reply
        result = reply.get("result", "failed")
        
        return result
    
    
    def stop(self):
        """Stop the client listener."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


class ClientManager:
    """Manages all 10 clients in a single process."""
    
    def __init__(self):
        self.clients: Dict[str, Client] = {}
        
        for client_id in config.CLIENT_IDS:
            self.clients[client_id] = Client(client_id)
        
        print(f"[ClientManager] Initialized {len(self.clients)} clients")
    
    
    def send_transaction(self, client_id: str, transaction: Tuple[str, str, int]) -> Optional[str]:
        """Send transaction from specific client."""
        if client_id not in self.clients:
            print(f"[ClientManager] Unknown client: {client_id}")
            return None
        
        client = self.clients[client_id]
        result = client.send_transaction(transaction)
        
        return result
    
    
    def send_transactions_parallel(self, transactions: List[Tuple[str, Tuple[str, str, int]]]):
        """Send multiple transactions in parallel from different clients."""
        threads = []
        results = {}
        
        def send_and_store(client_id, transaction):
            result = self.send_transaction(client_id, transaction)
            results[client_id] = result
        
        for client_id, transaction in transactions:
            thread = threading.Thread(
                target=send_and_store,
                args=(client_id, transaction),
                daemon=True
            )
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        return results
    
    
    def send_transactions_sequential(self, client_id: str, transactions: List[Tuple[str, str, int]]):
        """Send multiple transactions from one client sequentially."""
        results = []
        
        for transaction in transactions:
            result = self.send_transaction(client_id, transaction)
            results.append(result)
            
            if result == "failed":
                if config.VERBOSE_LOGGING:
                    print(f"[ClientManager] Client {client_id} transaction failed (insufficient balance)")
        
        return results
    
    
    def stop_all(self):
        """Stop all clients."""
        for client in self.clients.values():
            client.stop()
        print("[ClientManager] All clients stopped")
