from typing import Dict, Tuple, Any
import config


class Database:
    """Key-value store for client balances."""
    
    def __init__(self):
        self.balances: Dict[str, int] = {}
        
        for client_id in config.CLIENT_IDS:
            self.balances[client_id] = config.INITIAL_BALANCE
    
    
    def get_balance(self, client_id: str) -> int:
        """Get client balance."""
        return self.balances.get(client_id, 0)
    
    
    def execute_transaction(self, transaction: Any) -> str:
        """Execute transaction, returns 'success' or 'failed'."""
        if transaction == "no-op":
            return "success"
        
        if not isinstance(transaction, (tuple, list)) or len(transaction) != 3:
            return "failed"
        
        sender, receiver, amount = transaction
        
        if sender not in self.balances or receiver not in self.balances:
            return "failed"
        
        if amount <= 0:
            return "failed"
        
        if self.balances[sender] < amount:
            return "failed"
        
        if sender == receiver:
            return "failed"
        
        self.balances[sender] -= amount
        self.balances[receiver] += amount
        
        return "success"
    
    
    def can_execute_transaction(self, transaction: Any) -> bool:
        """Check if transaction can be executed without executing it."""
        if transaction == "no-op":
            return True
        
        if not isinstance(transaction, (tuple, list)) or len(transaction) != 3:
            return False
        
        sender, receiver, amount = transaction
        
        if sender not in self.balances or receiver not in self.balances:
            return False
        
        if amount <= 0:
            return False
        
        if self.balances[sender] < amount:
            return False
        
        if sender == receiver:
            return False
        
        return True
    
    
    def get_all_balances(self) -> Dict[str, int]:
        """Get copy of all client balances."""
        return self.balances.copy()
    
    
    def print_database(self) -> None:
        """Print current database state."""
        print("\n" + "="*50)
        print("DATABASE STATE")
        print("="*50)
        
        for client_id in sorted(self.balances.keys()):
            balance = self.balances[client_id]
            print(f"Client {client_id}: {balance} units")
        
        print("="*50 + "\n")
    
    
    def get_database_snapshot(self) -> str:
        """Get string representation of database state."""
        lines = []
        for client_id in sorted(self.balances.keys()):
            balance = self.balances[client_id]
            lines.append(f"{client_id}:{balance}")
        
        return ", ".join(lines)
    
    
    def reset_database(self) -> None:
        """Reset all balances to initial state."""
        for client_id in config.CLIENT_IDS:
            self.balances[client_id] = config.INITIAL_BALANCE
    
    
    def __str__(self) -> str:
        return self.get_database_snapshot()
    
    
    def __repr__(self) -> str:
        return f"Database({self.get_database_snapshot()})"
