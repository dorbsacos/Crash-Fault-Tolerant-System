from typing import Dict, List, Any, Optional, Tuple
from utils import format_ballot, format_transaction


class Status:
    """Transaction status labels."""
    ACCEPTED = "A"
    COMMITTED = "C"
    EXECUTED = "E"
    NO_STATUS = "X"


class LogEntry:
    """Single log entry tracking progression: Accept → Commit → Execute."""
    
    def __init__(self, sequence_number: int, ballot: Tuple[int, int], transaction: Any):
        self.sequence_number = sequence_number
        self.ballot = ballot
        self.transaction = transaction
        self.status = Status.NO_STATUS
    
    def __str__(self) -> str:
        return (f"Seq={self.sequence_number} "
                f"Ballot={format_ballot(self.ballot)} "
                f"Txn={format_transaction(self.transaction)} "
                f"Status={self.status}")


class RequestLog:
    """Log for tracking requests and their status at a node."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.entries: Dict[int, LogEntry] = {}
        self.message_history: List[Dict[str, Any]] = []
    
    
    def add_entry(self, sequence_number: int, ballot: Tuple[int, int], transaction: Any) -> None:
        """Add or update log entry. Update only if ballot is higher."""
        if sequence_number not in self.entries:
            self.entries[sequence_number] = LogEntry(sequence_number, ballot, transaction)
        else:
            if ballot > self.entries[sequence_number].ballot:
                self.entries[sequence_number].ballot = ballot
                self.entries[sequence_number].transaction = transaction
    
    
    def update_status(self, sequence_number: int, status: str) -> None:
        """Update status. Monotonic transitions only: X → A → C → E."""
        if sequence_number in self.entries:
            current_status = self.entries[sequence_number].status
            
            status_order = {
                None: 0,
                Status.ACCEPTED: 1,
                Status.COMMITTED: 2,
                Status.EXECUTED: 3
            }
            
            if status_order.get(status, 0) > status_order.get(current_status, 0):
                self.entries[sequence_number].status = status
    
    
    def mark_accepted(self, sequence_number: int) -> None:
        self.update_status(sequence_number, Status.ACCEPTED)
    
    
    def mark_committed(self, sequence_number: int) -> None:
        self.update_status(sequence_number, Status.COMMITTED)
    
    
    def mark_executed(self, sequence_number: int) -> None:
        self.update_status(sequence_number, Status.EXECUTED)
    
    
    def get_status(self, sequence_number: int) -> str:
        """Get status of sequence number."""
        if sequence_number in self.entries:
            return self.entries[sequence_number].status
        return Status.NO_STATUS
    
    
    def get_entry(self, sequence_number: int) -> Optional[LogEntry]:
        """Get log entry for sequence number."""
        return self.entries.get(sequence_number)
    
    
    def get_highest_sequence(self) -> int:
        """Get highest sequence number in log, or 0 if empty."""
        if not self.entries:
            return 0
        return max(self.entries.keys())
    
    
    def log_message(self, message: Dict[str, Any]) -> None:
        """Log message to history for PrintLog."""
        self.message_history.append(message.copy())
    
    
    def print_log(self) -> None:
        """Print log entries and all messages."""
        print("\n" + "="*60)
        print(f"LOG FOR NODE {self.node_id}")
        print("="*60)
        
        if not self.entries:
            print("Log is empty.")
        else:
            for seq_num in sorted(self.entries.keys()):
                entry = self.entries[seq_num]
                print(entry)
        
        print("="*60)
        
        if self.message_history:
            total_messages = len(self.message_history)
            
            print(f"\nMESSAGE HISTORY (All {total_messages} messages)")
            print("-"*60)
            for i, msg in enumerate(self.message_history, 1):
                msg_type = msg.get("type", "UNKNOWN")
                print(f"{i}. {msg_type}: {msg}")
            print("="*60 + "\n")
    
    
    def print_status(self, sequence_number: int) -> None:
        """Print status for specific sequence number."""
        status = self.get_status(sequence_number)
        print(f"Node {self.node_id} - Sequence {sequence_number}: {status}")
    
    
    def get_all_entries_sorted(self) -> List[LogEntry]:
        """Get all log entries sorted by sequence number."""
        return [self.entries[seq] for seq in sorted(self.entries.keys())]
    
    
    def has_sequence(self, sequence_number: int) -> bool:
        """Check if sequence number exists in log."""
        return sequence_number in self.entries
    
    
    def can_execute_sequence(self, sequence_number: int) -> bool:
        """Check if sequence can execute (all lower sequences must be executed)."""
        for seq in range(1, sequence_number):
            if seq not in self.entries:
                return False
            if self.entries[seq].status != Status.EXECUTED:
                return False
        
        return True
    
    
    def get_accept_log(self) -> List[Tuple[Tuple[int, int], int, Any]]:
        """Get AcceptLog for PROMISE: entries with status A, C, or E."""
        accept_log = []
        for entry in self.entries.values():
            if entry.status != Status.NO_STATUS:
                accept_log.append((entry.ballot, entry.sequence_number, entry.transaction))
        
        return sorted(accept_log, key=lambda x: x[1])
    
    
    def __str__(self) -> str:
        return f"RequestLog(node={self.node_id}, entries={len(self.entries)})"
    
    
    def __repr__(self) -> str:
        return f"RequestLog(node={self.node_id}, entries={len(self.entries)}, highest_seq={self.get_highest_sequence()})"
