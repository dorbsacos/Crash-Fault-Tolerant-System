import multiprocessing
import signal
import sys
import time
import csv
from typing import List, Dict, Optional, Tuple

import config
from node import PaxosNode
from client import ClientManager


def run_node(node_id: int, control_queue, response_queue):
    """Run a single Paxos node in separate process."""
    try:
        node = PaxosNode(node_id, control_queue, response_queue)
        node.start()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"[Node {node_id}] Shutting down")
        sys.exit(0)


class TestRunner:
    """Manages automated test execution from CSV file."""
    
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        self.test_sets = []
        self.client_manager = None
        self.current_leader_id = 1
        
        self.processes = {}
        self.control_queues = {}
        self.response_queues = {}
        self.disconnected_nodes = set()
    
    def parse_csv(self):
        """Parse CSV file into test sets."""
        print(f"[TestRunner] Parsing {self.csv_file}")
        
        with open(self.csv_file, 'r') as f:
            reader = csv.DictReader(f)
            current_set = None
            
            for row in reader:
                set_num = row['Set Number'].strip()
                transaction = row['Transactions'].strip()
                live_nodes = row['Live Nodes'].strip()
                
                if set_num:
                    if current_set:
                        self.test_sets.append(current_set)
                    
                    current_set = {
                        'set_number': int(set_num),
                        'live_nodes': self._parse_live_nodes(live_nodes),
                        'transactions': []
                    }
                
                if transaction:
                    current_set['transactions'].append(transaction)
            
            if current_set:
                self.test_sets.append(current_set)
        
        print(f"[TestRunner] Loaded {len(self.test_sets)} test sets\n")
    
    def _parse_live_nodes(self, live_nodes_str: str) -> List[int]:
        """Parse '[n1, n2, n3]' into [1, 2, 3]."""
        nodes_str = live_nodes_str.strip('[]')
        node_parts = [n.strip() for n in nodes_str.split(',')]
        node_ids = []
        
        for part in node_parts:
            if part.startswith('n'):
                node_ids.append(int(part[1:]))
        
        return sorted(node_ids)
    
    def _parse_transaction(self, txn_str: str) -> Optional[Tuple[str, str, int]]:
        """Parse '(A, B, 5)' into ('A', 'B', 5)."""
        if txn_str == "LF":
            return None
        
        txn_str = txn_str.strip('()')
        parts = [p.strip() for p in txn_str.split(',')]
        
        if len(parts) == 3:
            return (parts[0], parts[1], int(parts[2]))
        
        return None
    
    def start_nodes(self):
        """Start all Paxos nodes."""
        print("[TestRunner] Starting nodes")
        
        for node_id in config.NODE_IDS:
            ctrl_queue = multiprocessing.Queue()
            resp_queue = multiprocessing.Queue()
            self.control_queues[node_id] = ctrl_queue
            self.response_queues[node_id] = resp_queue
            
            p = multiprocessing.Process(
                target=run_node,
                args=(node_id, ctrl_queue, resp_queue),
                name=f"Node-{node_id}"
            )
            p.start()
            self.processes[node_id] = p
            time.sleep(0.2)
        
        print("[TestRunner] Waiting for leader election")
        time.sleep(3)
    
    def send_control_command(self, node_id: int, command: str):
        """Send control command to node."""
        if node_id in self.control_queues:
            self.control_queues[node_id].put({"command": command})
        else:
            print(f"[TestRunner] WARNING: Node {node_id} not found")
    
    def query_actual_leader(self) -> Optional[int]:
        """Query all live nodes to find actual Paxos leader."""
        for node_id in config.NODE_IDS:
            if node_id not in self.disconnected_nodes:
                try:
                    while True:
                        self.response_queues[node_id].get_nowait()
                except:
                    pass
        
        for node_id in config.NODE_IDS:
            if node_id not in self.disconnected_nodes:
                self.send_control_command(node_id, "query_leader")
        
        time.sleep(0.2)
        
        for node_id in config.NODE_IDS:
            if node_id not in self.disconnected_nodes:
                try:
                    response = self.response_queues[node_id].get_nowait()
                    if response.get("is_leader"):
                        return node_id
                except:
                    pass
        
        return None
    
    def adjust_node_states(self, live_nodes: List[int]):
        """Reconnect/disconnect nodes to match live_nodes specification."""
        changed = False
        leader_affected = False
        
        for node_id in config.NODE_IDS:
            if node_id in live_nodes:
                if node_id in self.disconnected_nodes:
                    self.send_control_command(node_id, "reconnect")
                    self.disconnected_nodes.remove(node_id)
                    print(f"  Reconnecting Node {node_id}")
                    changed = True
            else:
                if node_id not in self.disconnected_nodes:
                    self.send_control_command(node_id, "disconnect")
                    self.disconnected_nodes.add(node_id)
                    print(f"  Disconnecting Node {node_id}")
                    if node_id == self.current_leader_id:
                        leader_affected = True
                    changed = True
        
        if changed:
            if leader_affected:
                print("  Waiting for leader election to complete...")
                time.sleep(7)
                for node_id in config.NODE_IDS:
                    if node_id not in self.disconnected_nodes:
                        self.current_leader_id = node_id
                        break
            else:
                print("  Waiting for system to stabilize...")
                time.sleep(2)
    
    def handle_LF(self):
        """Handle Leader Failure command by disconnecting actual leader."""
        actual_leader = self.query_actual_leader()
        
        if actual_leader:
            self.current_leader_id = actual_leader
            print(f"\n  [LF] Failing actual leader Node {actual_leader}")
        else:
            print(f"\n  [LF] No leader found, using expected Node {self.current_leader_id}")
        
        self.send_control_command(self.current_leader_id, "disconnect")
        self.disconnected_nodes.add(self.current_leader_id)
        
        print("  [LF] Waiting for new leader election")
        time.sleep(3)
        
        new_leader = self.query_actual_leader()
        if new_leader:
            self.current_leader_id = new_leader
            print(f"  [LF] New leader elected: Node {new_leader}\n")
        else:
            print(f"  [LF] Election complete (no clear leader yet)\n")
    
    def send_transaction(self, txn_str: str, txn_num: int) -> Optional[str]:
        """Send single transaction via client."""
        transaction = self._parse_transaction(txn_str)
        
        if transaction is None:
            return None
        
        sender, receiver, amount = transaction
        result = self.client_manager.send_transaction(sender, transaction)
        
        if result == "success":
            print(f"  [{txn_num}] {sender} -> {receiver} ${amount}: SUCCESS")
        elif result == "committed":
            print(f"  [{txn_num}] {sender} -> {receiver} ${amount}: COMMITTED (will be executed)")
        elif result == "failed":
            print(f"  [{txn_num}] {sender} -> {receiver} ${amount}: FAILED")
        else:
            print(f"  [{txn_num}] {sender} -> {receiver} ${amount}: TIMEOUT")
        
        time.sleep(0.2)
        return result
    
    def pause_all_timers(self):
        """Pause all node timers."""
        for node_id in self.control_queues:
            self.send_control_command(node_id, "pause_timer")
        time.sleep(0.3)
    
    def resume_all_timers(self):
        """Resume all node timers."""
        for node_id in self.control_queues:
            self.send_control_command(node_id, "resume_timer")
        time.sleep(0.3)
    
    def show_results(self, set_num: int):
        """Interactive prompt for viewing results."""
        print(f"\n[Set {set_num}] Complete")
        print("\nAvailable commands:")
        print("  printdb <node_id>       - Show database state")
        print("  printlog <node_id>      - Show transaction log")
        print("  printstatus <node_id> <seq> - Show status of sequence")
        print("  printview <node_id>     - Show NEW-VIEW messages")
        print("  next                    - Continue to next set")
        
        while True:
            try:
                cmd = input("\n> ").strip().lower()
                
                if not cmd:
                    continue
                
                parts = cmd.split()
                command = parts[0]
                
                if command == "next":
                    break
                elif command == "printdb" and len(parts) == 2:
                    node_id = int(parts[1])
                    if node_id in self.control_queues:
                        self.control_queues[node_id].put({"command": "print_db"})
                        time.sleep(0.5)
                    else:
                        print(f"Invalid node ID: {node_id}")
                elif command == "printlog" and len(parts) == 2:
                    node_id = int(parts[1])
                    if node_id in self.control_queues:
                        self.control_queues[node_id].put({"command": "print_log"})
                        time.sleep(0.5)
                    else:
                        print(f"Invalid node ID: {node_id}")
                elif command == "printstatus" and len(parts) == 3:
                    node_id = int(parts[1])
                    seq_num = int(parts[2])
                    if node_id in self.control_queues:
                        self.control_queues[node_id].put({
                            "command": "print_status",
                            "sequence_number": seq_num
                        })
                        time.sleep(0.5)
                    else:
                        print(f"Invalid node ID: {node_id}")
                elif command == "printview" and len(parts) == 2:
                    node_id = int(parts[1])
                    if node_id in self.control_queues:
                        self.control_queues[node_id].put({"command": "print_view"})
                        time.sleep(0.5)
                    else:
                        print(f"Invalid node ID: {node_id}")
                else:
                    print("Invalid command. Use: printdb/printlog/printstatus/printview <node_id> or next")
                    
            except ValueError:
                print("Invalid input. Use: printdb/printlog/printstatus/printview <node_id> or next")
            except KeyboardInterrupt:
                print("\nUse 'next' to continue")
            except Exception as e:
                print(f"Error: {e}")
    
    def execute_test_set(self, test_set: dict):
        """Execute one test set."""
        set_num = test_set['set_number']
        live_nodes = test_set['live_nodes']
        transactions = test_set['transactions']
        
        print(f"\nTest Set {set_num}")
        print(f"Live Nodes: {live_nodes}")
        print(f"Transactions: {len(transactions)}")
        
        self.adjust_node_states(live_nodes)
        
        actual_leader = self.query_actual_leader()
        if actual_leader:
            print(f"Current Leader: Node {actual_leader}")
            self.current_leader_id = actual_leader
        else:
            print(f"Current Leader: None (no quorum)")
        
        txn_count = 0
        for i, txn_str in enumerate(transactions, 1):
            if txn_str == "LF":
                self.handle_LF()
            else:
                result = self.send_transaction(txn_str, i)
                if result is not None:
                    txn_count += 1
        
        time.sleep(3)
        
        self.pause_all_timers()
        self.show_results(set_num)
        self.resume_all_timers()
    
    def run_all_tests(self):
        """Execute all test sets."""
        print("\n[TestRunner] Initializing system\n")
        
        self.start_nodes()
        
        print("[TestRunner] Creating clients")
        self.client_manager = ClientManager()
        time.sleep(1)
        
        print("\n[TestRunner] Ready to run tests")
        print("Press ENTER to start first set")
        input()
        
        for test_set in self.test_sets:
            self.execute_test_set(test_set)
        
        print("\n[TestRunner] All test sets complete")
        print("Press ENTER to exit")
        input()
        
        self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        print("\n[TestRunner] Cleaning up")
        
        if self.client_manager:
            self.client_manager.stop_all()
        
        for node_id, p in self.processes.items():
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
                if p.is_alive():
                    p.kill()
        
        print("[TestRunner] Done")


def start_nodes_only():
    """Start nodes in manual mode (no CSV testing)."""
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    
    print(f"[System] Starting {config.NUM_NODES} Paxos nodes")
    print("[System] Manual mode - no automated testing")
    
    processes = []
    
    for node_id in config.NODE_IDS:
        ctrl_queue = multiprocessing.Queue()
        resp_queue = multiprocessing.Queue()
        
        p = multiprocessing.Process(
            target=run_node,
            args=(node_id, ctrl_queue, resp_queue),
            name=f"Node-{node_id}"
        )
        p.start()
        processes.append(p)
        time.sleep(0.2)
    
    print(f"[System] All nodes started. Press Ctrl+C to stop\n")
    
    try:
        while True:
            time.sleep(1)
            
            for p in processes:
                if not p.is_alive():
                    print(f"[System] WARNING: {p.name} stopped unexpectedly")
                    
    except KeyboardInterrupt:
        print("\n[System] Stopping all nodes")
        
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2)
        
        print("[System] All nodes stopped")


def main():
    """Main entry point."""
    multiprocessing.set_start_method('spawn', force=True)
    
    if len(sys.argv) < 2:
        print("Distributed Banking System - Manual Mode")
        print("Usage: python main.py <csv_file> to run automated tests\n")
        start_nodes_only()
    else:
        csv_file = sys.argv[1]
        
        print("Distributed Banking System - Test Runner")
        print(f"Test File: {csv_file}\n")
        
        runner = TestRunner(csv_file)
        runner.parse_csv()
        
        try:
            runner.run_all_tests()
        except KeyboardInterrupt:
            print("\n[TestRunner] Interrupted")
            runner.cleanup()
        except Exception as e:
            print(f"\n[TestRunner] ERROR: {e}")
            import traceback
            traceback.print_exc()
            runner.cleanup()


if __name__ == "__main__":
    main()
