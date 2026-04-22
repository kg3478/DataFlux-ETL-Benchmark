import re

def transform_record(record):
    '''
    record tuple: (id, first_name, last_name, email, phone, department_code, salary, hire_date, performance_score, is_active, address, city, country)
    '''
    # 1. Normalize Email
    email = record[3].lower() if record[3] else ''
    
    # 2. Map Department Code
    dept_map = {'ENG': 'Engineering', 'MKT': 'Marketing', 'SAL': 'Sales', 'HR': 'Human Resources', 'FIN': 'Finance'}
    dept = dept_map.get(record[5], 'Unknown')
    
    # 3. Salary Tier Classification
    sal = record[6]
    tier = 'High' if sal > 100000 else 'Medium' if sal > 60000 else 'Low'
    
    # 4. Phone Cleaning
    phone = re.sub(r'\D', '', record[4]) if record[4] else ''
    
    # 5. Score Normalization (0-1)
    score = record[8] / 100.0 if record[8] else 0.0
    
    # 6. Active Flag Derivation
    active_str = str(record[9]).lower()
    is_active = 1 if active_str in ['yes', 'y', '1', 'true'] else 0
    
    return (
        record[0], record[1], record[2], email, phone, dept, tier, record[7], score, is_active, record[10], record[11], record[12]
    )