#!/usr/bin/env python3

import os, json, requests, pandas as pd
from datetime import datetime, timedelta, date
from dateutil import tz
import time
from supabase import create_client, Client

# Config
BASE = "https://api.alegra.com/api/v1"
LOCAL_TZ = tz.gettz("America/Bogota")
CONFIG = json.load(open("price_lists_config.json", encoding="utf8"))

DISTRIBUTOR_SET = set(CONFIG["distributor_lists"])
MAYORISTA_SET = set(CONFIG["mayorista_lists"])
MAX_MONTHS_WITHOUT_PURCHASE = 6

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

def auth():
    return (os.getenv("ALEGRA_API_EMAIL"), os.getenv("ALEGRA_API_TOKEN"))

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def paginate(endpoint, params=None):
    params = params or {}
    params.update({"limit": 30, "start": 0})
    
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
    
    if TEST_MODE:
        max_items = int(os.getenv("TEST_MAX_CONTACTS" if endpoint == "contacts" else "TEST_MAX_ITEMS", "100"))
    else:
        max_items = float('inf')
    
    count = 0
    
    while True:
        try:
            r = requests.get(f"{BASE}/{endpoint}", auth=auth(), params=params, timeout=30)
            r.raise_for_status()
            batch = r.json()
            
            if not batch:
                break
                
            for item in batch:
                yield item
                count += 1
                if TEST_MODE and count >= max_items:
                    return
                
            if len(batch) < 30:
                break
                
            params["start"] += 30
            time.sleep(0.1)
            
        except requests.exceptions.RequestException:
            break

def truncate_tables():
    """Truncar tablas para rebuild completo"""
    try:
        supabase = get_supabase_client()
        for table in (
            "sales_processed",
            "clients_last_purchase",
            "sync_state",
        ):
            supabase.table(table).delete().neq("id", 0).execute()
    except Exception as e:
        print(f"Error en truncate: {e}")
        raise

def save_state(sync_date: date):
    try:
        supabase = get_supabase_client()
        supabase.table("sync_state").upsert({
            "id": 1,
            "last_sync": sync_date.isoformat(),
            "updated_at": datetime.now(LOCAL_TZ).isoformat()
        }, on_conflict="id").execute()
    except Exception as e:
        print(f"Error guardando estado: {e}")

def load_state():
    try:
        supabase = get_supabase_client()
        result = supabase.table("sync_state").select("*").eq("id", 1).execute()
        if result.data:
            return result.data[0]
        return {"last_sync": None}
    except Exception as e:
        print(f"Error cargando estado: {e}")
        return {"last_sync": None}

def get_existing_sales_ids():
    try:
        supabase = get_supabase_client()
        result = supabase.table("sales_processed").select("sale_id, sale_type").execute()
        existing = set()
        for row in result.data:
            existing.add(f"{row['sale_id']}_{row['sale_type']}")
        return existing
    except Exception as e:
        print(f"Error obteniendo ventas existentes: {e}")
        return set()

def save_new_sales(sales_list):
    try:
        supabase = get_supabase_client()
        
        if not sales_list:
            return
        
        unique_sales = {}
        for sale in sales_list:
            key = f"{sale['sale_id']}_{sale['type']}"
            unique_sales[key] = {
                "sale_id": str(sale["sale_id"]),
                "sale_type": sale["type"],
                "client_id": str(sale["client_id"]),
                "sale_date": sale["date"],
                "price_list_id": sale["price_list_id"]
            }
        
        records = list(unique_sales.values())
        
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table("sales_processed").upsert(batch, on_conflict="sale_id,sale_type").execute()
        
    except Exception as e:
        print(f"Error guardando ventas: {e}")

def extract_location_info(contact_data):
    city = ""
    state = ""
    
    if "address" in contact_data and contact_data["address"]:
        address = contact_data["address"]
        city = address.get("city", "")
        state = address.get("state", "")
    
    if not city and "city" in contact_data:
        city = contact_data.get("city", "")
    if not state and "state" in contact_data:
        state = contact_data.get("state", "")
    
    return {"city": city.strip() if city else "", "state": state.strip() if state else ""}

def fetch_contacts():
    contacts = {}
    
    for c in paginate("contacts"):
        cid = int(c["id"])
        price_list = c.get("priceList") or {}
        price_id = str(price_list.get("id", "")) if price_list.get("id") is not None else None
        location = extract_location_info(c)
        
        contacts[cid] = {
            "price_id": price_id,
            "name": c.get("name", ""),
            "email": c.get("email", ""),
            "city": location["city"],
            "state": location["state"]
        }
    
    return contacts

def fetch_all_sales():
    sales_dict = {}
    
    for inv in paginate("invoices"):
        sale_key = f"{inv['id']}_invoice"
        if sale_key not in sales_dict:
            client_id = int(inv["client"]["id"])
            price_list_id = None
            if "priceList" in inv and inv["priceList"]:
                price_list_id = str(inv["priceList"]["id"])

            sales_dict[sale_key] = {
                "sale_id": inv["id"],
                "client_id": client_id,
                "date": inv["date"],
                "price_list_id": price_list_id,
                "type": "invoice"
            }
    
    for rem in paginate("remissions"):
        sale_key = f"{rem['id']}_remission"
        if sale_key not in sales_dict:
            client_id = int(rem["client"]["id"])
            price_list_id = None
            if "priceList" in rem and rem["priceList"]:
                price_list_id = str(rem["priceList"]["id"])
            
            sales_dict[sale_key] = {
                "sale_id": rem["id"],
                "client_id": client_id,
                "date": rem["date"],
                "price_list_id": price_list_id,
                "type": "remission"
            }
    
    return list(sales_dict.values())

def fetch_new_sales(since: date | None):
    existing_ids = get_existing_sales_ids()
    sales_dict = {}
    
    params = {}
    if since:
        params["date[from]"] = since.isoformat()
    
    for inv in paginate("invoices", params=params):
        sale_key = f"{inv['id']}_invoice"
        if sale_key not in existing_ids and sale_key not in sales_dict:
            client_id = int(inv["client"]["id"])
            price_list_id = None
            if "priceList" in inv and inv["priceList"]:
                price_list_id = str(inv["priceList"]["id"])

            sales_dict[sale_key] = {
                "sale_id": inv["id"],
                "client_id": client_id,
                "date": inv["date"],
                "price_list_id": price_list_id,
                "type": "invoice"
            }
    
    for rem in paginate("remissions", params=params):
        sale_key = f"{rem['id']}_remission"
        if sale_key not in existing_ids and sale_key not in sales_dict:
            client_id = int(rem["client"]["id"])
            price_list_id = None
            if "priceList" in rem and rem["priceList"]:
                price_list_id = str(rem["priceList"]["id"])
            
            sales_dict[sale_key] = {
                "sale_id": rem["id"],
                "client_id": client_id,
                "date": rem["date"],
                "price_list_id": price_list_id,
                "type": "remission"
            }
    
    return list(sales_dict.values())

def get_last_purchases_from_sales(sales):
    last_purchases = {}
    
    for sale in sales:
        client_id = sale["client_id"]
        sale_date = datetime.fromisoformat(sale["date"]).date()
        
        if client_id not in last_purchases or sale_date > last_purchases[client_id]["date"]:
            last_purchases[client_id] = {
                "date": sale_date,
                "price_list_id": sale["price_list_id"]
            }
    
    return last_purchases

def get_last_purchases_from_db():
    try:
        supabase = get_supabase_client()
        result = supabase.table("sales_processed").select(
            "client_id, sale_date, price_list_id"
        ).order("client_id", desc=False).order("sale_date", desc=True).execute()
        
        last_purchases = {}
        for row in result.data:
            client_id = int(row["client_id"])
            sale_date = row["sale_date"]
            
            if client_id not in last_purchases:
                last_purchases[client_id] = {
                    "date": sale_date,
                    "price_list_id": row["price_list_id"]
                }
        
        return last_purchases
        
    except Exception as e:
        print(f"Error obteniendo últimas compras: {e}")
        return {}

def category_from_price(price_id: str | None):
    if price_id in DISTRIBUTOR_SET:
        return "Distribuidores"
    if price_id in MAYORISTA_SET:
        return "Mayoristas"
    return None

def is_within_timeframe(last_purchase_date: date, max_months: int = MAX_MONTHS_WITHOUT_PURCHASE):
    today = datetime.now(LOCAL_TZ).date()
    cutoff_date = today - timedelta(days=max_months * 30)
    return last_purchase_date >= cutoff_date

def save_to_supabase(df):
    try:
        supabase = get_supabase_client()
        
        if df.empty:
            return
        
        records = df.to_dict('records')
        
        for record in records:
            if 'fecha_ultima_compra' in record:
                record['fecha_ultima_compra'] = record['fecha_ultima_compra'].isoformat()
            
            for key in ['created_at', 'updated_at', 'id']:
                if key in record:
                    del record[key]
        
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table("clients_last_purchase").upsert(batch, on_conflict="cliente_id").execute()
        
    except Exception as e:
        print(f"Error guardando en Supabase: {e}")
        raise

def build_full_report(contacts, all_sales):
    last_purchases = get_last_purchases_from_sales(all_sales)
    today = datetime.now(LOCAL_TZ).date()
    rows = []
    
    for client_id, purchase_info in last_purchases.items():
        contact_info = contacts.get(client_id, {})
        
        if not contact_info:
            continue
        
        price_id = purchase_info["price_list_id"] or contact_info.get("price_id")
        categoria = category_from_price(price_id)
        
        if not categoria:
            continue
        
        last_dt = purchase_info["date"]
        days_without_purchase = (today - last_dt).days
        
        if not is_within_timeframe(last_dt):
            continue
        
        rows.append({
            "cliente_id": str(client_id),
            "cliente_nombre": contact_info.get("name", ""),
            "cliente_email": contact_info.get("email", ""),
            "cliente_ciudad": contact_info.get("city", ""),
            "cliente_estado": contact_info.get("state", ""),
            "categoria": categoria,
            "lista_precio_id": price_id,
            "fecha_ultima_compra": last_dt,
            "dias_sin_compra": days_without_purchase,
        })
    
    if rows:
        df = pd.DataFrame(rows)
        save_to_supabase(df)

def update_client_reports(contacts, new_sales):
    all_last_purchases = get_last_purchases_from_db()
    updated_clients = set(sale["client_id"] for sale in new_sales)
    
    if not updated_clients:
        return
    
    today = datetime.now(LOCAL_TZ).date()
    rows = []
    
    for client_id in updated_clients:
        if client_id not in all_last_purchases:
            continue
            
        purchase_info = all_last_purchases[client_id]
        contact_info = contacts.get(client_id, {})
        
        if not contact_info:
            continue
        
        price_id = purchase_info["price_list_id"] or contact_info.get("price_id")
        categoria = category_from_price(price_id)
        
        if not categoria:
            continue
        
        last_dt = datetime.fromisoformat(purchase_info["date"]).date()
        days_without_purchase = (today - last_dt).days
        
        if not is_within_timeframe(last_dt):
            continue
        
        rows.append({
            "cliente_id": str(client_id),
            "cliente_nombre": contact_info.get("name", ""),
            "cliente_email": contact_info.get("email", ""),
            "cliente_ciudad": contact_info.get("city", ""),
            "cliente_estado": contact_info.get("state", ""),
            "categoria": categoria,
            "lista_precio_id": price_id,
            "fecha_ultima_compra": last_dt,
            "dias_sin_compra": days_without_purchase,
        })
    
    if rows:
        df_updated = pd.DataFrame(rows)
        save_to_supabase(df_updated)

def main():
    # Detectar si es rebuild desde variable de entorno
    rebuild_mode = os.getenv("REBUILD_MODE", "false").lower() == "true"
    
    if rebuild_mode:
        truncate_tables()
        contacts = fetch_contacts()
        all_sales = fetch_all_sales()
        save_new_sales(all_sales)
        build_full_report(contacts, all_sales)
        save_state(datetime.now(LOCAL_TZ).date())
        
    else:
        # Modo incremental
        state = load_state()
        since = None
        if state["last_sync"]:
            since = datetime.fromisoformat(state["last_sync"]).date()
        else:
            since = date(2020, 1, 1)

        contacts = fetch_contacts()
        new_sales = fetch_new_sales(since)

        if new_sales:
            save_new_sales(new_sales)
            time.sleep(2)
            update_client_reports(contacts, new_sales)

        save_state(datetime.now(LOCAL_TZ).date())

if __name__ == "__main__":
    main()
