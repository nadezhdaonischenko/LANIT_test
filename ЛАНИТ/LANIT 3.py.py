import os
import requests 
import pandas as pd
from time import time
from sqlalchemy import create_engine 

URL = "https://run.mob-edu.ru/webhook/da-test-sample" 

# ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ API

try: 
    response = requests.get(URL, timeout=10) 
    response.raise_for_status() 
    data = response.json() 
    print("Данные успешно получены из API")

except requests.exceptions.Timeout: 
    print("[Ошибка] Превышено время ожидания ответа от сервера") 
except requests.exceptions.ConnectionError: 
    print("[Ошибка] Не удалось установить соединение с сервером") 
except requests.exceptions.HTTPError as e: 
    print(f"[Ошибка HTTP] {e}") 
except requests.exceptions.RequestException as e: 
    print(f"[Ошибка запроса] {e}") 
except ValueError: 
    print("[Ошибка] Некорректный JSON в ответе API")
    
    time.sleep(2)
   
    
# ПРЕОБРАЗОВАНИЕ ДАННЫХ В ТАБЛИЦЫ
# 1. КЛИЕНТЫ (clients)
try: 
    clients = [] 
    for item in data: 
        clients.append({ 
            "client_id": int(item["ID"]), 
            "title": item.get("TITLE"), 
            "first_name": item.get("NAME"), 
            "last_name": item.get("LAST_NAME"), 
            "status_id": item.get("STATUS_ID"), 
            "source_id": item.get("SOURCE_ID"), 
            "client_type": item.get("UF_CLIENT_TYPE"), 
            "preferred_contact_method": item.get("UF_CONTACT_METHOD") 
            }) 
        df_clients = pd.DataFrame(clients) 
    print("Таблица основных данных клиентов успешно создана")

except Exception as e: 
    print(f"Ошибка при обработке clients: {e}") 

#  2. ТЕЛЕФОНЫ КЛИЕНТОВ (phones)

try: 
    phones = [] 
    for item in data: 
        client_id = int(item["ID"]) 
        for phone in item.get("PHONE", []): 
            phones.append({
                "phone_id": int(phone["ID"]), 
                "client_id": client_id, 
                "phone_type": phone.get("TYPE_ID"), 
                "phone_number": phone.get("VALUE"), 
                "phone_category": phone.get("VALUE_TYPE") 
                }) 
        df_phones = pd.DataFrame(phones) 
    print("Таблица телефонов клиентов успешно создана") 
except Exception as e: 
    print(f"Ошибка при обработке phones: {e}") 

# 3.  ЭЛЕКТРОННАЯ ПОЧТА КЛИЕНТОВ (emails)
try: 
    emails = [] 
    for item in data: 
        client_id = int(item["ID"]) 
        for email in item.get("EMAIL", []): 
            emails.append({
                "email_id": int(email["ID"]), 
                "client_id": client_id, 
                "email_type": email.get("TYPE_ID"), 
                "email_address": email.get("VALUE"), 
                "email_category": email.get("VALUE_TYPE") 
                }) 
            df_emails = pd.DataFrame(emails) 
    print("Таблица данных электронной почты успешно создана") 
                
except Exception as e: 
    print(f"Ошибка при обработке emails: {e}") 
        
# ИТОГИ 

print("\nCLIENTS") 
print(df_clients.head())

print("\nPHONES") 
print(df_phones.head())

print("\nEMAILS")
print(df_emails.head())

# ПОДКЛЮЧЕНИЕ К POSTGRESQL
 
DB_USER = "postgres" 
DB_PASSWORD = os.getenv("parole_postgresql", "")
DB_HOST = "localhost" 
DB_PORT = "5432" 
DB_NAME = "postgres" 

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}" ) 

print("Подключение к PostgreSQL успешно выполнено")

# ЗАГРУЗКА ДАННЫХ В БАЗУ 

try: 
    df_clients.to_sql("clients", engine, if_exists="append", index=False) 
    print("Таблица clients успешно загружена") 
except Exception as e: 
    print(f"Ошибка при загрузке clients: {e}") 

try: 
    df_phones.to_sql("client_phones", engine, if_exists="append", index=False) 
    print("Таблица client_phones успешно загружена") 
except Exception as e: 
    print(f"Ошибка при загрузке client_phones: {e}") 

try: 
    df_emails.to_sql("client_emails", engine, if_exists="append", index=False) 
    print("Таблица client_emails успешно загружена") 
except Exception as e: 
    print(f"Ошибка при загрузке client_emails: {e}")

