# bulk_data_generator.py (complete working version)
import json
import random
import string
import bcrypt
import logging
from datetime import datetime, timedelta
from faker import Faker
from tqdm import tqdm
import configparser

class CasinoDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.config = configparser.ConfigParser()
        self.config.read('casino_admin.ini')
        self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(
            filename='data_generator.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def _generate_password(self):
        chars = string.ascii_letters + string.digits + "!@#$%^&*"
        password = [
            random.choice(string.ascii_uppercase),
            random.choice(string.digits),
            random.choice("!@#$%^&*")
        ] + [random.choice(chars) for _ in range(9)]
        random.shuffle(password)
        return ''.join(password)

    def generate_users(self, count=1000):
        users = []
        roles = ['user'] * 85 + ['operator'] * 10 + ['admin'] * 5
        domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'casino.test']
        
        for _ in tqdm(range(count), desc="Generating Users"):
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            user = {
                '_id': self.fake.uuid4(),
                'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1,99)}@{random.choice(domains)}",
                'password': bcrypt.hashpw(self._generate_password().encode(), bcrypt.gensalt()).decode(),
                'role': random.choice(roles),
                'balance': abs(round(random.gauss(5000, 3000), 2)),
                'active': random.choices([True, False], weights=[95, 5])[0],
                'created_at': self.fake.date_time_between(start_date='-2y', end_date='now'),
                'updated_at': datetime.now(),
                'metadata': {
                    'ip': self.fake.ipv4(),
                    'last_device': random.choice(['Windows', 'MacOS', 'iOS', 'Android']),
                    'vip_status': random.choices([True, False], weights=[5, 95])[0]
                }
            }
            users.append(user)
            
        self._save_to_json(users, 'users.json')
        return users

    def generate_login_logs(self, users, logs_per_user=15):
        logs = []
        for user in tqdm(users, desc="Generating Login Logs"):
            base_date = user['created_at']
            for _ in range(random.randint(1, logs_per_user)):
                log = {
                    '_id': self.fake.uuid4(),
                    'user_id': user['_id'],
                    'success': random.choices([True, False], weights=[85, 15])[0],
                    'timestamp': self.fake.date_time_between(start_date=base_date, end_date='now'),
                    'ip': self.fake.ipv4(),
                    'user_agent': self.fake.user_agent(),
                    'location': {
                        'city': self.fake.city(),
                        'country': self.fake.country_code()
                    }
                }
                logs.append(log)
        self._save_to_json(logs, 'login_logs.json')
        return logs

    def generate_transactions(self, users, transactions_per_user=50):
        transactions = []
        game_types = ['blackjack', 'slots', 'roulette', 'poker']
        
        for user in tqdm(users, desc="Generating Transactions"):
            balance = user['balance']
            current_date = user['created_at']
            
            for _ in range(random.randint(10, transactions_per_user)):
                tx_date = self.fake.date_time_between(start_date=current_date, end_date='now')
                tx_type = random.choices(
                    ['deposit', 'withdraw', 'game'],
                    weights=[15, 10, 75]
                )[0]
                
                if tx_type == 'game':
                    amount = abs(round(random.gauss(balance * 0.05, balance * 0.02), 2))
                    outcome = random.choices(['win', 'loss'], weights=[40, 60])[0]
                    if outcome == 'loss':
                        amount = -amount
                else:
                    amount = round(random.uniform(10, 5000), 2)
                    if tx_type == 'withdraw':
                        amount = -amount
                
                transaction = {
                    '_id': self.fake.uuid4(),
                    'user_id': user['_id'],
                    'type': tx_type,
                    'amount': amount,
                    'balance_after': balance + amount,
                    'date': tx_date,
                    'game_type': random.choice(game_types) if tx_type == 'game' else None,
                    'description': f"{tx_type.capitalize()} transaction",
                    'device': random.choice(['mobile', 'desktop']),
                    'ip': self.fake.ipv4()
                }
                transactions.append(transaction)
                balance += amount
                
        self._save_to_json(transactions, 'transactions.json')
        return transactions

    def generate_admin_logs(self, users):
        admin_logs = []
        admins = [u for u in users if u['role'] == 'admin']
        
        for _ in tqdm(range(len(admins) * 10), desc="Generating Admin Logs"):
            admin = random.choice(admins)
            log = {
                '_id': self.fake.uuid4(),
                'user_id': admin['_id'],
                'email': admin['email'],
                'action': random.choice(['user_edit', 'config_change', 'reset_password']),
                'timestamp': self.fake.date_time_between(
                    start_date=admin['created_at'], 
                    end_date='now'
                ),
                'ip': self.fake.ipv4(),
                'details': {
                    'target_user': random.choice(users)['email'],
                    'changes': {'field': random.choice(['balance', 'status', 'role'])}
                }
            }
            admin_logs.append(log)
        
        self._save_to_json(admin_logs, 'admin_logs.json')
        return admin_logs

    def _save_to_json(self, data, filename):
        try:
            with open(filename, 'w') as f:
                for item in tqdm(data, desc=f"Saving {filename}"):
                    f.write(json.dumps(item, default=str) + '\n')
            logging.info(f"Saved {len(data)} records to {filename}")
        except Exception as e:
            logging.error(f"Error saving {filename}: {str(e)}")
            raise

    def generate_all_data(self):
        try:
            print("Starting casino data generation...")
            users = self.generate_users(5000)
            login_logs = self.generate_login_logs(users)
            transactions = self.generate_transactions(users)
            admin_logs = self.generate_admin_logs(users)
            
            print("\nData Generation Complete:")
            print(f"- Users: {len(users)}")
            print(f"- Login Logs: {len(login_logs)}")
            print(f"- Transactions: {len(transactions)}")
            print(f"- Admin Logs: {len(admin_logs)}")
            print("\nFiles created: users.json, login_logs.json, transactions.json, admin_logs.json")
            
        except Exception as e:
            logging.error(f"Data generation failed: {str(e)}")
            print("Error occurred - check data_generator.log")

if __name__ == "__main__":
    generator = CasinoDataGenerator()
    generator.generate_all_data()