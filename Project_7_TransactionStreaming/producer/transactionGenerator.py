from faker import Faker
import random
from datetime import datetime as date

fake=Faker()
# counter=0
class TransactionGenerator:
    def __init__(self):
        self.counter = 0

    def generate_transaction(self):
        self.counter += 1
        return {
            "transaction_id": self.counter,
            "user_id": fake.uuid4(),
            "amount": round(random.uniform(10.0, 1000.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "JPY", "AUD"]),
            "timestamp": date.now().isoformat(' ','seconds'),
            "merchant": fake.company(),
            "category": random.choice(["Groceries", "Electronics", "Clothing", "Dining", "Travel"]),
            "status": random.choice(["Pending", "Completed", "Failed"])

        }
