import sys
import os
import json
import re
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import json_util, ObjectId
from datetime import datetime, timedelta
import bcrypt
import getpass
import configparser
import platform
import time
import random
import string

class CasinoAdminDesktop:
    CONFIG_FILE = 'casino_admin.ini'
    DEFAULT_ADMIN = {
        'email': 'admin@casino.com',
        'password': 'Admin123!',
        'role': 'admin',
        'balance': 0,
        'active': True
    }
    BATCH_SIZE = 1000

    def clear_screen(self):
        """Clear console screen cross-platform"""
        os.system('cls' if platform.system() == 'Windows' else 'clear')

    def __init__(self):
        self.clear_screen()
        print("Initializing Casino Admin System...")
        time.sleep(0.5)
        self.load_config()
        self.connect_to_mongodb()
        self.initialize_database()
        self.current_user = None
        self.ensure_indexes()

    def load_config(self):
        """Load or create configuration file"""
        self.config = configparser.ConfigParser()
        if os.path.exists(self.CONFIG_FILE):
            try:
                self.config.read(self.CONFIG_FILE)
                print("✓ Configuration loaded")
            except Exception as e:
                print(f"Error loading configuration: {str(e)}")
                self.create_default_config()
        else:
            self.create_default_config()
            
    def create_default_config(self):
        """Create default configuration"""
        self.config['MONGODB'] = {
            'host': 'localhost',
            'port': '27017',
            'database': 'casino_db'
        }
        self.config['APP'] = {
            'title': 'Casino Admin System',
            'version': '1.0.0',
            'timeout': '300'
        }
        self.save_config()
        print("✓ Default configuration created")

    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.CONFIG_FILE, 'w') as configfile:
                self.config.write(configfile)
            print("✓ Configuration saved")
        except Exception as e:
            print(f"Error saving configuration: {str(e)}")

    def connect_to_mongodb(self):
        """Connect to MongoDB with error handling"""
        try:
            self.client = MongoClient(
                host=self.config['MONGODB']['host'],
                port=int(self.config['MONGODB']['port']),
                serverSelectionTimeoutMS=5000
            )
            self.client.server_info()
            self.db = self.client[self.config['MONGODB']['database']]
            print(f"✓ Connected to MongoDB at {self.config['MONGODB']['host']}:{self.config['MONGODB']['port']}")
        except Exception as e:
            print(f"✗ Failed to connect to MongoDB: {str(e)}")
            self.configure_mongodb()

    def configure_mongodb(self):
        """Configure MongoDB connection settings"""
        self.clear_screen()
        print("╔════════════════════════════════╗")
        print("║    MongoDB Configuration       ║")
        print("╚════════════════════════════════╝\n")
        
        self.config['MONGODB']['host'] = input("MongoDB Host [localhost]: ") or 'localhost'
        self.config['MONGODB']['port'] = input("MongoDB Port [27017]: ") or '27017'
        self.config['MONGODB']['database'] = input("Database Name [casino_db]: ") or 'casino_db'
        self.save_config()
        self.connect_to_mongodb()

    def initialize_database(self):
        """Initialize database collections"""
        try:
            self.users = self.db.users
            self.transactions = self.db.transactions
            self.login_logs = self.db.login_logs
            self.admin_logs = self.db.admin_logs
            self.games = self.db.games

            if not self.users.find_one({'email': self.DEFAULT_ADMIN['email']}):
                self.create_admin_user()
            print("✓ Database initialized")
        except Exception as e:
            print(f"✗ Database initialization failed: {str(e)}")
            sys.exit(1)

    def create_admin_user(self):
        """Create default admin user"""
        admin_data = {
            **self.DEFAULT_ADMIN,
            'password': bcrypt.hashpw(self.DEFAULT_ADMIN['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        self.users.insert_one(admin_data)
        print(f"✓ Created admin user: {self.DEFAULT_ADMIN['email']}")
        print("⚠ Default password: Admin123! (change this immediately)")

    def ensure_indexes(self):
        """Create database indexes for performance"""
        try:
            self.users.create_index([('email', ASCENDING)], unique=True)
            self.transactions.create_index([('user_id', ASCENDING)])
            self.transactions.create_index([('date', DESCENDING)])
            self.login_logs.create_index([('user_id', ASCENDING)])
            self.login_logs.create_index([('timestamp', DESCENDING)])
            self.admin_logs.create_index([('timestamp', DESCENDING)])
            print("✓ Database indexes created")
        except Exception as e:
            print(f"✗ Error creating indexes: {str(e)}")

    def log_action(self, action, details=None):
        """Log admin actions"""
        if not self.current_user:
            return
            
        log_entry = {
            'user_id': self.current_user['_id'],
            'email': self.current_user['email'],
            'action': action,
            'details': details,
            'timestamp': datetime.now(),
            'ip': '127.0.0.1'
        }
        self.admin_logs.insert_one(log_entry)

    def login(self) -> bool:
        """Handle user login"""
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║     CASINO ADMIN LOGIN     ║")
        print("╚════════════════════════════╝\n")
        
        email = input("Email: ")
        password = getpass.getpass("Password: ")
        
        user = self.users.find_one({'email': email})
        if not user:
            print("\n✗ User not found!")
            input("Press Enter to try again...")
            return False
            
        if not user.get('active', True):
            print("\n✗ Account disabled. Contact system administrator.")
            input("Press Enter to try again...")
            return False
            
        try:
            if bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
                self.current_user = user
                self.log_login_attempt(user['_id'], True)
                print(f"\n✓ Welcome, {user['email']} ({user['role'].upper()})")
                input("\nPress Enter to continue...")
                return True
            else:
                self.log_login_attempt(user['_id'], False)
                print("\n✗ Invalid password!")
                input("Press Enter to try again...")
                return False
        except Exception as e:
            print(f"\n✗ Login error: {str(e)}")
            input("Press Enter to try again...")
            return False

    def log_login_attempt(self, user_id, success):
        """Log login attempts"""
        log_entry = {
            'user_id': user_id,
            'success': success,
            'timestamp': datetime.now(),
            'ip': '127.0.0.1'
        }
        self.login_logs.insert_one(log_entry)

    def show_main_menu(self):
        """Display main menu and handle user input"""
        while True:
            self.clear_screen()
            print("╔════════════════════════════╗")
            print("║       MAIN MENU            ║")
            print("╠════════════════════════════╣")
            print("║ 1. User Management         ║")
            print("║ 2. Transaction Management  ║")
            print("║ 3. System Reports          ║")
            print("║ 4. System Configuration    ║")
            print("║ 5. Change Password         ║")
            print("║ 6. Logout                  ║")
            print("║ 7. Exit                    ║")
            print("╚════════════════════════════╝")
            
            choice = input("\nSelect option (1-7): ")
            
            if choice == '1':
                self.user_management_menu()
            elif choice == '2':
                self.transaction_management_menu()
            elif choice == '3':
                self.reports_menu()
            elif choice == '4':
                self.system_configuration_menu()
            elif choice == '5':
                self.change_password()
            elif choice == '6':
                self.logout()
                return
            elif choice == '7':
                self.clear_screen()
                print("Goodbye!")
                sys.exit()
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def logout(self):
        """Handle user logout"""
        self.log_action("logout")
        self.current_user = None
        self.clear_screen()
        print("You have been logged out.")
        time.sleep(1)

    def user_management_menu(self):
        """User management submenu"""
        while True:
            self.clear_screen()
            print("╔════════════════════════════╗")
            print("║    USER MANAGEMENT         ║")
            print("╠════════════════════════════╣")
            print("║ 1. List Users              ║")
            print("║ 2. Add New User            ║")
            print("║ 3. Edit User               ║")
            print("║ 4. Delete User             ║")
            print("║ 5. Reset User Password     ║")
            print("║ 6. Back to Main Menu       ║")
            print("╚════════════════════════════╝")
            
            choice = input("\nSelect option (1-6): ")
            
            if choice == '1':
                self.list_users()
            elif choice == '2':
                self.add_user()
            elif choice == '3':
                self.edit_user()
            elif choice == '4':
                self.delete_user()
            elif choice == '5':
                self.reset_user_password()
            elif choice == '6':
                return
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def transaction_management_menu(self):
        """Transaction management submenu"""
        while True:
            self.clear_screen()
            print("╔════════════════════════════════╗")
            print("║    TRANSACTION MANAGEMENT      ║")
            print("╠════════════════════════════════╣")
            print("║ 1. View User Transactions      ║")
            print("║ 2. Add Manual Transaction      ║")
            print("║ 3. Recent Transactions Report  ║")
            print("║ 4. Back to Main Menu           ║")
            print("╚════════════════════════════════╝")
            
            choice = input("\nSelect option (1-4): ")
            
            if choice == '1':
                self.view_transactions()
            elif choice == '2':
                self.add_transaction()
            elif choice == '3':
                self.recent_transactions_report()
            elif choice == '4':
                return
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def reports_menu(self):
        """Reports submenu"""
        while True:
            self.clear_screen()
            print("╔════════════════════════════╗")
            print("║       SYSTEM REPORTS       ║")
            print("╠════════════════════════════╣")
            print("║ 1. User Activity Report    ║")
            print("║ 2. Admin Logs              ║")
            print("║ 3. Deposit/Withdraw Report ║")
            print("║ 4. Export Data             ║")
            print("║ 5. Back to Main Menu       ║")
            print("╚════════════════════════════╝")
            
            choice = input("\nSelect option (1-5): ")
            
            if choice == '1':
                self.user_activity_report()
            elif choice == '2':
                self.view_admin_logs()
            elif choice == '3':
                self.deposit_withdraw_report()
            elif choice == '4':
                self.export_data()
            elif choice == '5':
                return
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def system_configuration_menu(self):
        """System configuration submenu"""
        while True:
            self.clear_screen()
            print("╔════════════════════════════════╗")
            print("║    SYSTEM CONFIGURATION        ║")
            print("╠════════════════════════════════╣")
            print("║ 1. MongoDB Configuration       ║")
            print("║ 2. System Settings             ║")
            print("║ 3. Import Data                 ║")
            print("║ 4. Back to Main Menu           ║")
            print("╚════════════════════════════════╝")
            
            choice = input("\nSelect option (1-4): ")
            
            if choice == '1':
                self.configure_mongodb()
            elif choice == '2':
                self.system_settings()
            elif choice == '3':
                self.import_data()
            elif choice == '4':
                return
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def list_users(self, page: int = 1, per_page: int = 20):
        """List users with pagination"""
        while True:
            self.clear_screen()
            skip = (page - 1) * per_page
            try:
                users = list(self.users.find().skip(skip).limit(per_page))
                total = self.users.count_documents({})
            except Exception as e:
                print(f"Error loading users: {str(e)}")
                input("Press Enter to continue...")
                return
            
            print(f"╔═══════════════════════════════════════════════════════════════╗")
            print(f"║                    USER LIST (Page {page})                      ║")
            print(f"╠═══════════════════════════════════════════════════════════════╣")
            print(f"║ {'ID':<4} {'Email':<25} {'Role':<8} {'Balance':<8} {'Status':<8} ║")
            print(f"╠═══════════════════════════════════════════════════════════════╣")
            
            for idx, user in enumerate(users, skip + 1):
                status = "Active" if user.get('active', True) else "Inactive"
                print(f"║ {idx:<4} {user['email'][:24]:<25} {user['role'][:7]:<8} ${user.get('balance', 0):<7.2f} {status:<8} ║")
            
            print(f"╚═══════════════════════════════════════════════════════════════╝")
            print(f"\nPage {page} of {max(1, (total + per_page - 1) // per_page)} ({total} total users)")
            
            print("\nActions: [N]ext, [P]revious, [V]iew details, [E]dit, [D]elete, [B]ack")
            nav = input("Choose action: ").lower()
            
            if nav == 'n' and page * per_page < total:
                page += 1
            elif nav == 'p' and page > 1:
                page -= 1
            elif nav == 'v':
                self.view_user_details(users, skip)
            elif nav == 'e':
                self.edit_user_from_list(users, skip)
            elif nav == 'd':
                self.delete_user_from_list(users, skip)
            elif nav == 'b':
                return
            else:
                print("Invalid option!")
                input("Press Enter to continue...")

    def view_user_details(self, users, skip):
        """View detailed user information"""
        try:
            user_idx = int(input("\nEnter user number to view details (0 to cancel): ")) - 1 - skip
            if 0 <= user_idx < len(users):
                user = users[user_idx]
                self.clear_screen()
                print("╔════════════════════════════════════════╗")
                print("║           USER DETAILS                 ║")
                print("╚════════════════════════════════════════╝\n")
                print(f"Email: {user['email']}")
                print(f"Role: {user['role']}")
                print(f"Balance: ${user.get('balance', 0):.2f}")
                print(f"Status: {'Active' if user.get('active', True) else 'Inactive'}")
                print(f"Created: {user.get('created_at', 'Unknown')}")
                print(f"Last Updated: {user.get('updated_at', 'Unknown')}")
                
                tx_count = self.transactions.count_documents({'user_id': user['_id']})
                last_login = self.login_logs.find_one(
                    {'user_id': user['_id'], 'success': True},
                    sort=[('timestamp', DESCENDING)]
                )
                
                print(f"\nTransaction Count: {tx_count}")
                if last_login:
                    print(f"Last Login: {last_login.get('timestamp', 'Unknown')}")
                
                self.log_action("view_user_details", {"user_id": str(user['_id'])})
                input("\nPress Enter to continue...")
        except (ValueError, IndexError):
            print("Invalid selection!")
            input("Press Enter to continue...")

    def add_user(self):
        """Add a new user"""
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║       ADD NEW USER         ║")
        print("╚════════════════════════════╝\n")
        
        email = input("Email: ")
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_regex, email):
            print("Invalid email format!")
            input("Press Enter to continue...")
            return
            
        if self.users.find_one({'email': email}):
            print("User with this email already exists!")
            input("Press Enter to continue...")
            return
            
        valid_roles = ['user', 'admin', 'operator']
        role = input(f"Role ({'/'.join(valid_roles)}): ").lower()
        if role not in valid_roles:
            print(f"Invalid role! Must be one of: {', '.join(valid_roles)}")
            input("Press Enter to continue...")
            return
            
        password = self.generate_random_password()
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        user_data = {
            'email': email,
            'password': hashed_password,
            'role': role,
            'balance': 0,
            'active': True,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        try:
            result = self.users.insert_one(user_data)
            if result.inserted_id:
                self.log_action("add_user", {"user_id": str(result.inserted_id), "email": email})
                print(f"\n✓ User created successfully!")
                print(f"Temporary password: {password}")
                print("\nMake sure to provide this password to the user securely.")
                input("\nPress Enter to continue...")
            else:
                print("\n✗ Error creating user!")
                input("Press Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error creating user: {str(e)}")
            input("Press Enter to continue...")

    def generate_random_password(self, length=12):
        """Generate a random secure password"""
        chars = string.ascii_letters + string.digits + "!@#$%^&*"
        password = [
            random.choice(string.ascii_uppercase),
            random.choice(string.ascii_lowercase),
            random.choice(string.digits),
            random.choice("!@#$%^&*")
        ]
        password.extend(random.choice(chars) for _ in range(length - 4))
        random.shuffle(password)
        return ''.join(password)

    def edit_user(self):
        """Edit an existing user"""
        user_id = self._select_user()
        if not user_id:
            return
        self._edit_user_by_id(user_id)
    
    def edit_user_from_list(self, users, skip):
        """Edit user directly from list view"""
        try:
            user_idx = int(input("\nEnter user number to edit (0 to cancel): ")) - 1 - skip
            if 0 <= user_idx < len(users):
                user = users[user_idx]
                self._edit_user_by_id(user['_id'])
            else:
                print("Invalid selection!")
                input("Press Enter to continue...")
        except (ValueError, IndexError):
            print("Invalid selection!")
            input("Press Enter to continue...")
    
    def _edit_user_by_id(self, user_id):
        """Edit user by ID"""
        user = self.users.find_one({'_id': user_id})
        if not user:
            print("User not found!")
            input("Press Enter to continue...")
            return
            
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║        EDIT USER           ║")
        print("╚════════════════════════════╝\n")
        
        print(f"Editing user: {user['email']}")
        print(f"Current role: {user['role']}")
        print(f"Current status: {'Active' if user.get('active', True) else 'Inactive'}")
        print(f"Current balance: ${user.get('balance', 0):.2f}")
        print("\nLeave field empty to keep current value.\n")
        
        valid_roles = ['user', 'admin', 'operator']
        new_role = input(f"New role ({'/'.join(valid_roles)}): ").lower()
        if new_role and new_role not in valid_roles:
            print(f"Invalid role! Must be one of: {', '.join(valid_roles)}")
            input("Press Enter to continue...")
            return
            
        status_input = input("Status (active/inactive): ").lower()
        new_status = None
        if status_input:
            if status_input == 'active':
                new_status = True
            elif status_input == 'inactive':
                new_status = False
            else:
                print("Invalid status! Must be 'active' or 'inactive'")
                input("Press Enter to continue...")
                return
                
        balance_input = input("New balance (numbers only): ")
        new_balance = None
        if balance_input:
            try:
                new_balance = float(balance_input)
                if new_balance < 0:
                    print("Balance cannot be negative!")
                    input("Press Enter to continue...")
                    return
            except ValueError:
                print("Invalid balance! Must be a number.")
                input("Press Enter to continue...")
                return
                
        update_data = {'updated_at': datetime.now()}
        if new_role:
            update_data['role'] = new_role
        if new_status is not None:
            update_data['active'] = new_status
        if new_balance is not None:
            old_balance = user.get('balance', 0)
            update_data['balance'] = new_balance
            
            if old_balance != new_balance:
                tx_type = "adjustment"
                amount = new_balance - old_balance
                self._create_balance_transaction(user_id, amount, tx_type, new_balance)
        
        try:
            result = self.users.update_one({'_id': user_id}, {'$set': update_data})
            if result.modified_count > 0:
                self.log_action("edit_user", {"user_id": str(user_id), "fields": list(update_data.keys())})
                print("\n✓ User updated successfully!")
            else:
                print("\n✓ No changes made to user.")
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error updating user: {str(e)}")
            input("Press Enter to continue...")

    def delete_user(self):
        """Delete an existing user"""
        user_id = self._select_user()
        if not user_id:
            return
        self._delete_user_by_id(user_id)
        
    def delete_user_from_list(self, users, skip):
        """Delete user directly from list view"""
        try:
            user_idx = int(input("\nEnter user number to delete (0 to cancel): ")) - 1 - skip
            if 0 <= user_idx < len(users):
                user = users[user_idx]
                self._delete_user_by_id(user['_id'])
            else:
                print("Invalid selection!")
                input("Press Enter to continue...")
        except (ValueError, IndexError):
            print("Invalid selection!")
            input("Press Enter to continue...")
    
    def _delete_user_by_id(self, user_id):
        """Delete user by ID"""
        user = self.users.find_one({'_id': user_id})
        if not user:
            print("User not found!")
            input("Press Enter to continue...")
            return
            
        if self.current_user and str(user_id) == str(self.current_user['_id']):
            print("You cannot delete your own account!")
            input("Press Enter to continue...")
            return
            
        if user['role'] == 'admin':
            admin_count = self.users.count_documents({'role': 'admin'})
            if admin_count <= 1:
                print("Cannot delete the last admin user!")
                input("Press Enter to continue...")
                return
                
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║       DELETE USER          ║")
        print("╚════════════════════════════╝\n")
        
        print(f"You are about to delete user: {user['email']}")
        print(f"Role: {user['role']}")
        print(f"Balance: ${user.get('balance', 0):.2f}")
        
        confirm = input("\nType 'DELETE' to confirm: ")
        if confirm != "DELETE":
            print("\nDeletion cancelled.")
            input("Press Enter to continue...")
            return
            
        try:
            result = self.users.update_one(
                {'_id': user_id}, 
                {'$set': {'active': False, 'deleted': True, 'deleted_at': datetime.now()}}
            )
            
            if result.modified_count > 0:
                self.log_action("delete_user", {"user_id": str(user_id), "email": user['email']})
                print("\n✓ User deleted successfully!")
            else:
                print("\n✗ Failed to delete user.")
                
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error deleting user: {str(e)}")
            input("Press Enter to continue...")

    def reset_user_password(self):
        """Reset a user's password"""
        user_id = self._select_user()
        if not user_id:
            return
            
        user = self.users.find_one({'_id': user_id})
        if not user:
            print("User not found!")
            input("Press Enter to continue...")
            return
            
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║    RESET USER PASSWORD     ║")
        print("╚════════════════════════════╝\n")
        
        print(f"Resetting password for: {user['email']}")
        confirm = input("\nProceed with password reset? (y/n): ").lower()
        
        if confirm != 'y':
            print("\nPassword reset cancelled.")
            input("Press Enter to continue...")
            return
            
        new_password = self.generate_random_password()
        hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        try:
            result = self.users.update_one(
                {'_id': user_id},
                {'$set': {'password': hashed_password, 'updated_at': datetime.now(), 'password_reset': True}}
            )
            
            if result.modified_count > 0:
                self.log_action("reset_password", {"user_id": str(user_id)})
                print("\n✓ Password reset successfully!")
                print(f"New temporary password: {new_password}")
                print("\nMake sure to provide this password to the user securely.")
            else:
                print("\n✗ Failed to reset password.")
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error resetting password: {str(e)}")
            input("Press Enter to continue...")

    def _create_balance_transaction(self, user_id, amount, tx_type, new_balance):
        """Create a transaction record for balance adjustment"""
        transaction_data = {
            'user_id': user_id,
            'type': tx_type,
            'amount': amount,
            'balance_after': new_balance,
            'description': f"Manual balance adjustment by admin {self.current_user['email']}",
            'date': datetime.now()
        }
        self.transactions.insert_one(transaction_data)
        self.log_action("create_transaction", {"user_id": str(user_id), "amount": amount, "type": tx_type})

    def _select_user(self):
        """Select a user by email or list selection"""
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║      SELECT USER           ║")
        print("╚════════════════════════════╝\n")
        
        search_email = input("Enter user email (or leave blank to list all users): ").strip()
        if search_email:
            user = self.users.find_one({'email': search_email})
            if user:
                return user['_id']
            print("User not found!")
            input("Press Enter to continue...")
            return None
        
        try:
            users = list(self.users.find().limit(20))
            if not users:
                print("No users found!")
                input("Press Enter to continue...")
                return None
                
            print("Recent users:\n")
            for idx, user in enumerate(users, 1):
                print(f"{idx}. {user['email']} ({user['role']})")
                
            choice = input("\nEnter user number (1-20) or 0 to cancel: ")
            if not choice.isdigit():
                print("Invalid input!")
                input("Press Enter to continue...")
                return None
                
            choice_idx = int(choice) - 1
            if choice_idx == -1:
                return None
                
            if 0 <= choice_idx < len(users):
                return users[choice_idx]['_id']
                
            print("Invalid selection!")
            input("Press Enter to continue...")
            return None
            
        except Exception as e:
            print(f"Error: {str(e)}")
            input("Press Enter to continue...")
            return None

    def view_transactions(self):
        """View transactions for a specific user"""
        user_id = self._select_user()
        if not user_id:
            return
            
        try:
            transactions = list(self.transactions.find({'user_id': user_id}).sort('date', DESCENDING).limit(50))
            self.clear_screen()
            print(f"╔══════════════════════════════════════════════════════════╗")
            print(f"║                  USER TRANSACTIONS                      ║")
            print(f"╠══════════════════════════════════════════════════════════╣")
            print(f"║ {'Date':<20} {'Type':<12} {'Amount':<10} {'Balance':<10} ║")
            for tx in transactions:
                date_str = tx['date'].strftime("%Y-%m-%d %H:%M")
                print(f"║ {date_str:<20} {tx['type'][:11]:<12} ${tx['amount']:<9.2f} ${tx['balance_after']:<9.2f} ║")
            print(f"╚══════════════════════════════════════════════════════════╝")
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Error loading transactions: {str(e)}")
            input("Press Enter to continue...")

    def add_transaction(self):
        """Add manual transaction"""
        user_id = self._select_user()
        if not user_id:
            return
            
        user = self.users.find_one({'_id': user_id})
        if not user:
            print("User not found!")
            input("Press Enter to continue...")
            return
            
        self.clear_screen()
        print("╔════════════════════════════════╗")
        print("║      ADD TRANSACTION           ║")
        print("╚════════════════════════════════╝\n")
        
        print(f"Current balance: ${user.get('balance', 0):.2f}")
        print("\nTransaction types: deposit, withdraw, adjustment")
        
        tx_type = input("Transaction type: ").lower()
        if tx_type not in ['deposit', 'withdraw', 'adjustment']:
            print("Invalid transaction type!")
            input("Press Enter to continue...")
            return
            
        try:
            amount = float(input("Amount: "))
            if amount <= 0:
                print("Amount must be positive!")
                input("Press Enter to continue...")
                return
        except ValueError:
            print("Invalid amount!")
            input("Press Enter to continue...")
            return
            
        description = input("Description: ")[:100]
        
        new_balance = user['balance'] 
        if tx_type == 'deposit':
            new_balance += amount
        elif tx_type == 'withdraw':
            if user['balance'] < amount:
                print("Insufficient funds!")
                input("Press Enter to continue...")
                return
            new_balance -= amount
        else:
            new_balance = amount
            
        try:
            self.users.update_one(
                {'_id': user_id},
                {'$set': {'balance': new_balance}}
            )
            
            self._create_balance_transaction(
                user_id,
                amount if tx_type != 'adjustment' else (new_balance - user['balance']),
                tx_type,
                new_balance
            )
            
            print("\n✓ Transaction added successfully!")
            input("Press Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error adding transaction: {str(e)}")
            input("Press Enter to continue...")

    def user_activity_report(self):
        """Generate user activity report"""
        try:
            self.clear_screen()
            print("╔════════════════════════════════════════╗")
            print("║         USER ACTIVITY REPORT          ║")
            print("╚════════════════════════════════════════╝\n")
            
            days = int(input("Enter days to report (7/30/90): ") or 7)
            cutoff_date = datetime.now() - timedelta(days=days)
            
            pipeline = [
                {'$match': {'timestamp': {'$gte': cutoff_date}}},
                {'$group': {
                    '_id': '$user_id',
                    'last_login': {'$max': '$timestamp'},
                    'success_count': {'$sum': {'$cond': ['$success', 1, 0]}},
                    'failed_count': {'$sum': {'$cond': ['$success', 0, 1]}}
                }},
                {'$sort': {'last_login': DESCENDING}}
            ]
            
            results = list(self.login_logs.aggregate(pipeline))
            
            print(f"\n{'Email':<25} {'Last Login':<20} {'Success':<8} {'Failed':<8}")
            print("-" * 65)
            for entry in results:
                user = self.users.find_one({'_id': entry['_id']})
                email = user['email'] if user else 'Deleted User'
                last_login = entry['last_login'].strftime("%Y-%m-%d %H:%M")
                print(f"{email[:24]:<25} {last_login:<20} {entry['success_count']:<8} {entry['failed_count']:<8}")
            
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Error generating report: {str(e)}")
            input("Press Enter to continue...")

    def view_admin_logs(self):
        """View admin action logs"""
        try:
            logs = list(self.admin_logs.find().sort('timestamp', DESCENDING).limit(50))
            self.clear_screen()
            print("╔════════════════════════════════════════════════╗")
            print("║               ADMIN ACTION LOGS               ║")
            print("╠════════════════════════════════════════════════╣")
            print(f"║ {'Timestamp':<19} {'Admin':<25} {'Action':<15} ║")
            for log in logs:
                timestamp = log['timestamp'].strftime("%Y-%m-%d %H:%M")
                print(f"║ {timestamp:<19} {log['email'][:24]:<25} {log['action'][:14]:<15} ║")
            print("╚════════════════════════════════════════════════╝")
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Error loading logs: {str(e)}")
            input("Press Enter to continue...")

    def system_settings(self):
        """Modify system settings"""
        self.clear_screen()
        print("╔════════════════════════════════╗")
        print("║      SYSTEM SETTINGS          ║")
        print("╚════════════════════════════════╝\n")
        
        print(f"Current Settings:")
        print(f"Session Timeout: {self.config['APP']['timeout']} seconds")
        
        new_timeout = input("\nNew session timeout (seconds, 300-3600): ")
        if new_timeout:
            try:
                timeout = int(new_timeout)
                if 300 <= timeout <= 3600:
                    self.config['APP']['timeout'] = str(timeout)
                    self.save_config()
                    print("\n✓ Settings updated!")
                else:
                    print("Timeout must be between 300 and 3600 seconds!")
            except ValueError:
                print("Invalid number format!")
        
        input("\nPress Enter to continue...")

    def change_password(self):
        """Change current user's password"""
        self.clear_screen()
        print("╔════════════════════════════╗")
        print("║      CHANGE PASSWORD       ║")
        print("╚════════════════════════════╝\n")
        
        if not self.current_user:
            print("You must be logged in to change your password.")
            input("Press Enter to continue...")
            return
        
        current_password = getpass.getpass("Current Password: ")
        if not bcrypt.checkpw(current_password.encode('utf-8'), self.current_user['password'].encode('utf-8')):
            print("\n✗ Current password is incorrect!")
            input("Press Enter to continue...")
            return
        
        new_password = getpass.getpass("New Password: ")
        confirm_password = getpass.getpass("Confirm New Password: ")
        
        if new_password != confirm_password:
            print("\n✗ New passwords do not match!")
            input("Press Enter to continue...")
            return
        
        if len(new_password) < 8:
            print("\n✗ Password must be at least 8 characters!")
            input("Press Enter to continue...")
            return
        if not re.search(r'[A-Z]', new_password):
            print("\n✗ Password must contain at least one uppercase letter!")
            input("Press Enter to continue...")
            return
        if not re.search(r'[a-z]', new_password):
            print("\n✗ Password must contain at least one lowercase letter!")
            input("Press Enter to continue...")
            return
        if not re.search(r'[0-9]', new_password):
            print("\n✗ Password must contain at least one digit!")
            input("Press Enter to continue...")
            return
        if not re.search(r'[!@#$%^&*]', new_password):
            print("\n✗ Password must contain at least one special character (!@#$%^&*)!")
            input("Press Enter to continue...")
            return
        
        hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        try:
            self.users.update_one(
                {'_id': self.current_user['_id']},
                {'$set': {'password': hashed_password, 'updated_at': datetime.now(), 'password_reset': False}}
            )
            self.log_action("change_password")
            print("\n✓ Password changed successfully!")
            input("Press Enter to continue...")
        except Exception as e:
            print(f"\n✗ Error changing password: {str(e)}")
            input("Press Enter to continue...")

    def recent_transactions_report(self):
        """Display recent transactions report"""
        try:
            transactions = list(self.transactions.find().sort('date', DESCENDING).limit(50))
            self.clear_screen()
            print("╔══════════════════════════════════════════════════════════╗")
            print("║                  RECENT TRANSACTIONS                     ║")
            print("╠══════════════════════════════════════════════════════════╣")
            print(f"║ {'Date':<20} {'User':<25} {'Type':<12} {'Amount':<10} ║")
            for tx in transactions:
                user = self.users.find_one({'_id': tx['user_id']})
                email = user['email'] if user else 'Deleted User'
                date_str = tx['date'].strftime("%Y-%m-%d %H:%M")
                print(f"║ {date_str:<20} {email[:24]:<25} {tx['type'][:11]:<12} ${tx['amount']:<9.2f} ║")
            print("╚══════════════════════════════════════════════════════════╝")
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Error loading transactions: {str(e)}")
            input("Press Enter to continue...")

    def export_data(self):
        """Export data to JSON file"""
        self.clear_screen()
        print("╔════════════════════════════════╗")
        print("║         EXPORT DATA            ║")
        print("╚════════════════════════════════╝\n")
        
        try:
            data = {
                'users': list(self.users.find()),
                'transactions': list(self.transactions.find())
            }
            
            file_path = input("Enter full path to save file: ")
            with open(file_path, 'w') as f:
                json.dump(data, f, default=json_util.default)
            
            print("\n✓ Data exported successfully!")
            self.log_action("export_data", {"file_path": file_path})
            input("Press Enter to continue...")
        except Exception as e:
            print(f"Export failed: {str(e)}")
            input("Press Enter to continue...")

    def import_data(self):
        """Import data from JSON file"""
        self.clear_screen()
        print("╔════════════════════════════════╗")
        print("║         IMPORT DATA           ║")
        print("╚════════════════════════════════╝\n")
        
        file_path = input("Enter full path to import file: ")
        if not os.path.exists(file_path):
            print("File not found!")
            input("Press Enter to continue...")
            return
            
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            if not all(key in data for key in ['users', 'transactions']):
                raise ValueError("Invalid data format")
                
            users_inserted = 0
            for user in data['users']:
                if not self.users.find_one({'email': user['email']}):
                    self.users.insert_one(user)
                    users_inserted += 1
                    
            transactions_inserted = 0
            for tx in data['transactions']:
                self.transactions.insert_one(tx)
                transactions_inserted += 1
                
            print(f"\nImport complete:")
            print(f"- {users_inserted} new users added")
            print(f"- {transactions_inserted} transactions added")
            self.log_action("import_data", {
                'file': file_path,
                'users': users_inserted,
                'transactions': transactions_inserted
            })
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Import failed: {str(e)}")
            input("Press Enter to continue...")

    def deposit_withdraw_report(self):
        """Generate deposit/withdraw report"""
        try:
            self.clear_screen()
            print("╔════════════════════════════════════════╗")
            print("║      DEPOSIT/WITHDRAW REPORT          ║")
            print("╚════════════════════════════════════════╝\n")
            
            days = int(input("Enter days to report (7/30/90): ") or 7)
            cutoff_date = datetime.now() - timedelta(days=days)
            
            pipeline = [
                {'$match': {
                    'date': {'$gte': cutoff_date},
                    'type': {'$in': ['deposit', 'withdraw']}
                }},
                {'$group': {
                    '_id': '$type',
                    'total_amount': {'$sum': '$amount'},
                    'count': {'$sum': 1}
                }},
                {'$sort': {'_id': ASCENDING}}
            ]
            
            results = list(self.transactions.aggregate(pipeline))
            
            print(f"\n{'Type':<15} {'Count':<10} {'Total Amount':<15}")
            print("-" * 40)
            for entry in results:
                print(f"{entry['_id'].capitalize():<15} {entry['count']:<10} ${entry['total_amount']:<15.2f}")
            
            input("\nPress Enter to continue...")
        except Exception as e:
            print(f"Error generating report: {str(e)}")
            input("Press Enter to continue...")

if __name__ == "__main__":
    app = CasinoAdminDesktop()
    if app.login():
        app.show_main_menu()