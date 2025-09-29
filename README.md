# TerraVault

**TerraVault** is an **online banking application** built as a semifinal requirement project.  
It provides APIs for user management, transactions, balances, notifications, logs, and points.

---

## 🔧 Features

- User registration, login, and authentication  
- Viewing and updating account balances  
- Transaction handling (deposit, withdrawal, transfer)  
- Notification service (alerts, emails, etc.)  
- Logs service (audit trails, activity logs)  
- Points / rewards handling  
- Service gateway / API router  

---

## 📁 Project Structure

Here’s a high-level view of the files in the repository:

| File | Purpose |
|---|---|
| `api_gateway.py` | Entrypoint / router for API requests |
| `authentication.py` | Handling login, tokens, sessions, etc. |
| `balance_service.py` | Managing user balances |
| `transactions.py` | Logic for money movement between accounts |
| `logs_service.py` | Recording activity and audit logs |
| `notifications_service.py` | Sending notifications / alerts to users |
| `points_service.py` | Handling reward / loyalty point system |
| `user_management.py` | Managing user profiles, registration, user data |

---

## 🛠 Requirements & Setup

### Prerequisites

- Python 3.x  
- Required libraries (e.g. Flask / FastAPI / whatever web framework your app uses)  
- Database (e.g. PostgreSQL, MySQL, SQLite — whichever you chose)  

### Installation & Running

1. Clone the repository:  
   ```bash
   git clone https://github.com/vinzhenryyy/TerraVault.git
   cd TerraVault
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
3. Setup environment variables / configuration (e.g. database URL, secret keys)
4. Initialize / migrate your database schema
5. Start the application:
   ```bash
   python api_gateway.py
6. Access APIs via http://localhost:PORT/ (adjust the port and host as configured)
