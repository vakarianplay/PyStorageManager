import argparse
import getpass
import psycopg2
import os
import yaml
import sys

def prompt_input(label, default=None, secret=False):
    prompt = f"{label}"
    if default:
        prompt += f" [{default}]"
    prompt += ": "
    return (getpass.getpass(prompt) if secret else input(prompt)) or default

def parse_args():
    parser = argparse.ArgumentParser(description="Database configurator")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--dbname")
    parser.add_argument("--company-name")
    parser.add_argument("--company-logo")
    parser.add_argument("--server-host")
    parser.add_argument("--server-port", type=int)
    return parser.parse_args()

def collect_inputs(args):
    config = {
        "database": {
            "host": args.host or prompt_input("PostgreSQL Host", "localhost"),
            "port": args.port or int(prompt_input("PostgreSQL Port", "5432")),
            "user": args.user or prompt_input("PostgreSQL Username", "postgres"),
            "password": args.password or prompt_input("PostgreSQL Password", secret=True),
            "name": args.dbname or prompt_input("Database Name")
        },
        "server": {
            "host": args.server_host or prompt_input("Server Host", "localhost"),
            "port": args.server_port or int(prompt_input("Server Port", "8080"))
        },
        "company": {
            "name": args.company_name or prompt_input("Company Name", "MyCompany"),
            "logo": args.company_logo or prompt_input("Company Logo (URL or path)", "logo.png")
        }
    }
    return config

def create_database(config):
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=config["database"]["user"],
            password=config["database"]["password"],
            host=config["database"]["host"],
            port=config["database"]["port"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE {config['database']['name']};")
        cur.close()
        conn.close()
        print(f"Database '{config['database']['name']}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{config['database']['name']}' already exists.")
    except Exception as e:
        print(f"Error while creating database: {e}")
        sys.exit(1)

def apply_sql_script(config, script_path="init_db.sql"):
    try:
        with open(script_path, "r", encoding="utf-8") as f:
            sql = f.read()

        conn = psycopg2.connect(
            dbname=config["database"]["name"],
            user=config["database"]["user"],
            password=config["database"]["password"],
            host=config["database"]["host"],
            port=config["database"]["port"]
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        print("SQL init script applied successfully.")
    except Exception as e:
        print(f"Error applying SQL script: {e}")
        sys.exit(1)

def save_config(config, path="config.yaml"):
    try:
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        print(f"Configuration saved to {path}")
    except Exception as e:
        print(f"Error saving config file: {e}")
        sys.exit(1)

def main():
    args = parse_args()
    config = collect_inputs(args)

    create_database(config)
    apply_sql_script(config)
    save_config(config)

if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
    # usage example:
    
#     python configure_db.py \
#   --host 127.0.0.1 \
#   --port 5432 \
#   --user postgres \
#   --password mypass \
#   --dbname mydb \
#   --server-host 0.0.0.0 \
#   --server-port 8080 \
#   --company-name "ExampleCorp" \
#   --company-logo "logo.png"