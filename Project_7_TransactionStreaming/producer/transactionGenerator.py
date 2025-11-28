from faker import Faker
import random
from datetime import datetime as date

fake=Faker()
# counter=0
class TransactionGenerator:
    def __init__(self,counterFile="counter.txt"):
        self.counterFile=counterFile
        self.counter=self.load_counter()

    def load_counter(self):
        try:
            with open (self.counterFile,mode='r') as file:
                line=file.read().strip()
                return int(line) if line.isdigit() else 0
        except Exception as e:
            print(e)
            return 0

    def save_file(self):
        try:
            with open (self.counterFile,mode='w') as file:
                file.write(str(self.counter))
                    
        except Exception as e:
            print(e)

    
    def generate_transaction(self):
        self.counter += 1
        self.save_file()
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
