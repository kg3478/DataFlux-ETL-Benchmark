import sqlite3
import random
from faker import Faker
import os

def generate_data(num_records=500000, db_path='source.db'):
    if os.path.exists(db_path):
        os.remove(db_path)
    fake = Faker()
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE source_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone TEXT,
        department_code TEXT,
        salary REAL,
        hire_date TEXT,
        performance_score INTEGER,
        is_active TEXT,
        address TEXT,
        city TEXT,
        country TEXT
    )''')
    
    records = []
    print(f"Generating {num_records} records in {db_path}...")
    for _ in range(num_records):
        records.append((
            fake.first_name(),
            fake.last_name(),
            fake.email().upper(), # Intentionally uppercase for transform
            fake.phone_number(),
            random.choice(['ENG', 'MKT', 'SAL', 'HR', 'FIN']),
            random.uniform(40000, 150000),
            str(fake.date_between(start_date='-10y', end_date='today')),
            random.randint(1, 100),
            random.choice(['Yes', 'No', 'Y', 'N', '1', '0']),
            fake.street_address(),
            fake.city(),
            fake.country()
        ))
        
        if len(records) >= 10000:
            c.executemany('''INSERT INTO source_records 
                (first_name, last_name, email, phone, department_code, salary, hire_date, performance_score, is_active, address, city, country) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', records)
            records = []
            conn.commit()
            
    if records:
        c.executemany('''INSERT INTO source_records 
                (first_name, last_name, email, phone, department_code, salary, hire_date, performance_score, is_active, address, city, country) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', records)
        conn.commit()
    conn.close()
    print("Data generation complete.")

if __name__ == '__main__':
    generate_data(10000) # Default to 10k for quick testing