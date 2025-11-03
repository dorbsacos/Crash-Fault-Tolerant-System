import time
from typing import List, Tuple, Any, Dict, Set


def is_ballot_greater(ballot1: Tuple[int, int], ballot2: Tuple[int, int]) -> bool:
    """Check if ballot1 is strictly greater than ballot2."""
    return ballot1 > ballot2


def is_ballot_greater_or_equal(ballot1: Tuple[int, int], ballot2: Tuple[int, int]) -> bool:
    """Check if ballot1 is greater than or equal to ballot2."""
    return ballot1 >= ballot2


def create_ballot(round_number: int, node_id: int) -> Tuple[int, int]:
    """Create ballot tuple (round_number, node_id)."""
    return (round_number, node_id)


def merge_accept_logs(accept_logs: List[List[Tuple[Tuple[int, int], int, Any]]]) -> List[Tuple[Tuple[int, int], int, Any]]:
    """Merge accept logs from different nodes, keeping highest ballot for each sequence."""
    merged: Dict[int, Tuple[Tuple[int, int], int, Any]] = {}
    
    for accept_log in accept_logs:
        for entry in accept_log:
            ballot, seq_num, _ = entry
            ballot = tuple(ballot)
            
            if seq_num not in merged or is_ballot_greater(ballot, merged[seq_num][0]):
                merged[seq_num] = (ballot, seq_num, entry[2])
    
    sorted_entries = sorted(merged.values(), key=lambda x: x[1])
    return sorted_entries


def fill_gaps_with_noops(accept_log: List[Tuple[Tuple[int, int], int, Any]], 
                         current_ballot: Tuple[int, int]) -> List[Tuple[Tuple[int, int], int, Any]]:
    """Fill sequence number gaps with no-ops. E.g., [1,3,5] → [1,2(no-op),3,4(no-op),5]."""
    if not accept_log:
        return []
    
    seq_numbers = [entry[1] for entry in accept_log]
    min_seq = min(seq_numbers)
    max_seq = max(seq_numbers)
    
    present_seqs = set(seq_numbers)
    
    complete_log = []
    for seq in range(min_seq, max_seq + 1):
        if seq in present_seqs:
            for entry in accept_log:
                if entry[1] == seq:
                    complete_log.append(entry)
                    break
        else:
            no_op_entry = (current_ballot, seq, "no-op")
            complete_log.append(no_op_entry)
    
    return complete_log


def get_highest_sequence_number(accept_log: List[Tuple[Tuple[int, int], int, Any]]) -> int:
    """Get highest sequence number from accept log, or 0 if empty."""
    if not accept_log:
        return 0
    
    return max(entry[1] for entry in accept_log)


def filter_accept_log_after_checkpoint(accept_log: List[Tuple[Tuple[int, int], int, Any]], 
                                        checkpoint_seq: int) -> List[Tuple[Tuple[int, int], int, Any]]:
    """Filter accept log to only include entries > checkpoint_seq."""
    return [entry for entry in accept_log if entry[1] > checkpoint_seq]


def is_valid_transaction(transaction: Any, sender_balance: int) -> Tuple[bool, str]:
    """Validate transaction: format, positive amount, sufficient balance."""
    if transaction == "no-op":
        return True, "no-op"
    
    if not isinstance(transaction, (tuple, list)) or len(transaction) != 3:
        return False, "invalid_format"
    
    sender, receiver, amount = transaction
    
    if amount <= 0:
        return False, "non_positive_amount"
    
    if sender_balance < amount:
        return False, "insufficient_balance"
    
    if sender == receiver:
        return False, "same_sender_receiver"
    
    return True, "valid"


def get_current_timestamp() -> float:
    """Get current timestamp in seconds since epoch."""
    return time.time()


def has_timeout_expired(start_time: float, timeout_ms: int) -> bool:
    """Check if timeout has expired."""
    elapsed_ms = (time.time() - start_time) * 1000
    return elapsed_ms >= timeout_ms


def get_elapsed_time_ms(start_time: float) -> float:
    """Get elapsed time in milliseconds since start_time."""
    return (time.time() - start_time) * 1000


def format_ballot(ballot: Tuple[int, int]) -> str:
    """Format ballot for display: (round,node_id)."""
    return f"({ballot[0]},{ballot[1]})"


def format_transaction(transaction: Any) -> str:
    """Format transaction for display: (sender,receiver,amount) or no-op."""
    if transaction == "no-op":
        return "no-op"
    
    if isinstance(transaction, (tuple, list)) and len(transaction) == 3:
        sender, receiver, amount = transaction
        return f"({sender},{receiver},{amount})"
    
    return str(transaction)


def format_accept_log_entry(entry: Tuple[Tuple[int, int], int, Any]) -> str:
    """Format accept log entry: seq=X ballot=(Y,Z) txn=(...)."""
    ballot, seq_num, transaction = entry
    return f"seq={seq_num} ballot={format_ballot(ballot)} txn={format_transaction(transaction)}"


def get_missing_sequences(accept_log: List[Tuple[Tuple[int, int], int, Any]], 
                         start: int, 
                         end: int) -> List[int]:
    """Get list of missing sequence numbers in range [start, end]."""
    present = {entry[1] for entry in accept_log}
    return [seq for seq in range(start, end + 1) if seq not in present]
