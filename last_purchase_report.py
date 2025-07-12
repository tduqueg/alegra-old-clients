#!/usr/bin/env python3
"""
Versión optimizada con filtros de ubicación y tiempo
Incluye opción de rebuild completo (truncate + reload)
"""

import os, json, requests, pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil import tz
import time
import argparse
from supabase import create_client, Client

# ------------------------------------------------------------------ config —

BASE = "https://api.alegra.com/api/v1"
LOCAL_TZ = tz.gettz("America/Bogota")
CONFIG = json.load(open("price_lists_config.json", encoding="utf8"))

DISTRIBUTOR_SET = set(CONFIG["distributor_lists"])
MAYORISTA_SET = set(CONFIG["mayorista_lists"])

# Configuración de filtros
MAX_MONTHS_WITHOUT_PURCHASE = 6
LOCATIONS_TO_TRACK = [
    "Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena",
    "Bucaramanga", "Pereira", "Manizales", "Ibagué", "Neiva"
]

# Configuración de Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# ----------------------------------------------------------- utilidades API —

def auth():
    return (
        os.getenv("ALEGRA_API_EMAIL"),
        os.getenv("ALEGRA_API_TOKEN"),
    )

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def paginate(endpoint, params=None):
    """Yields each item of a paginated Alegra endpoint."""
    params = params or {}
    params.update({"limit": 30, "start": 0})
    
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
    
    if TEST_MODE:
        if endpoint == "contacts":
            max_items = int(os.getenv("TEST_MAX_CONTACTS", "500"))
        else:
            max_items = int(os.getenv("TEST_MAX_ITEMS", "100"))
    else:
        max_items = float('inf')
    
    count = 0
    
    while True:
        print(f"Obteniendo {endpoint} - página {params['start']//30 + 1}")
        try:
            r = requests.get(f"{BASE}/{endpoint}", auth=auth(),
                             params=params, timeout=30)
            r.raise_for_status()
            batch = r.json()
            
            if not batch:
                break
                
            for item in batch:
                yield item
                count += 1
                if TEST_MODE and count >= max_items:
                    print(f"🧪 MODO TEST: Limitado a {max_items} registros para {endpoint}")
                    return
                
            if len(batch) < 30:
                break
                
            params["start"] += 30
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            break

# ----------------------------------------------------------- manejo estado —

def truncate_tables():
    """Truncar todas las tablas para rebuild completo"""
    try:
        supabase = get_supabase_client()
        
        print("🗑️  Truncando tablas...")
        
        # Truncar tabla de ventas procesadas
        supabase.table("sales_processed").delete().neq("id", "").execute()
        print("   ✓ sales_processed truncada")
        
        # Truncar tabla de reportes de clientes
        supabase.table("clients_last_purchase").delete().neq("id", "").execute()
        print("   ✓ clients_last_purchase truncada")
        
        # Resetear estado de sincronización
        supabase.table("sync_state").delete().neq("id", "").execute()
        print("   ✓ sync_state reseteado")
        
        print("✅ Truncate completado")
        
    except Exception as e:
        print(f"❌ Error en truncate: {e}")
        raise

def save_state(sync_date: date):
    """Guardar estado en Supabase"""
    try:
        supabase = get_supabase_client()
        
        supabase.table("sync_state").upsert({
            "id": 1,
            "last_sync": sync_date.isoformat(),
            "updated_at": datetime.now(LOCAL_TZ).isoformat()
        }, on_conflict="id").execute()
        
        print("✓ Estado guardado en Supabase")
    except Exception as e:
        print(f"Error guardando estado: {e}")

def load_state():
    """Cargar estado desde Supabase"""
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
    """Obtener IDs de ventas ya procesadas"""
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
    """Guardar nuevas ventas en la tabla sales_processed"""
    try:
        supabase = get_supabase_client()
        
        if not sales_list:
            return
        
        # Eliminar duplicados usando un diccionario
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
        
        # Insertar en lotes
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table("sales_processed").upsert(batch, on_conflict="sale_id,sale_type").execute()
        
        print(f"✓ {len(records)} ventas únicas guardadas en sales_processed")
        
    except Exception as e:
        print(f"Error guardando ventas: {e}")

# ------------------------------------------------------------- extracción —

def extract_location_info(contact_data):
    """Extraer información de ubicación del contacto"""
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
    
    return {
        "city": city.strip() if city else "",
        "state": state.strip() if state else ""
    }

def fetch_contacts():
    """Obtiene todos los contactos con sus price lists y ubicación"""
    contacts = {}
    contact_count = 0
    
    for c in paginate("contacts"):
        contact_count += 1
        cid = c["id"]
        
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
    
    print(f"✓ Obtenidos {contact_count} contactos")
    return contacts

def fetch_all_sales():
    """Obtiene TODAS las ventas (para rebuild completo)"""
    sales_dict = {}
    
    # Facturas
    print("Obteniendo TODAS las facturas...")
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
    
    print(f"✓ {len([s for s in sales_dict.values() if s['type'] == 'invoice'])} facturas obtenidas")
    
    # Remisiones
    print("Obteniendo TODAS las remisiones...")
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
    
    total_remissions = len([s for s in sales_dict.values() if s['type'] == 'remission'])
    print(f"✓ {total_remissions} remisiones obtenidas")
    
    sales = list(sales_dict.values())
    print(f"✓ Total ventas: {len(sales)}")
    
    return sales

def fetch_new_sales(since: date | None):
    """Obtiene solo las ventas nuevas (modo incremental)"""
    existing_ids = get_existing_sales_ids()
    print(f"✓ {len(existing_ids)} ventas ya procesadas")
    
    sales_dict = {}
    new_count = 0
    
    params = {}
    if since:
        params["date[from]"] = since.isoformat()
    
    # Facturas
    print("Obteniendo facturas nuevas...")
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
            new_count += 1
    
    print(f"✓ {new_count} facturas nuevas")
    
    # Remisiones
    remission_new_count = 0
    print("Obteniendo remisiones nuevas...")
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
            remission_new_count += 1
    
    print(f"✓ {remission_new_count} remisiones nuevas")
    
    sales = list(sales_dict.values())
    print(f"✓ Total ventas nuevas: {len(sales)}")
    
    return sales

def get_last_purchases_from_sales(sales):
    """Calcular últimas compras directamente desde las ventas"""
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
    """Obtener últimas compras desde la base de datos"""
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
        
        print(f"✓ Obtenidas últimas compras de {len(last_purchases)} clientes")
        return last_purchases
        
    except Exception as e:
        print(f"Error obteniendo últimas compras: {e}")
        return {}

# ----------------------------------------------------- categorización —

def category_from_price(price_id: str | None):
    if price_id in DISTRIBUTOR_SET:
        return "Distribuidores"
    if price_id in MAYORISTA_SET:
        return "Mayoristas"
    return None

def is_within_timeframe(last_purchase_date: date, max_months: int = MAX_MONTHS_WITHOUT_PURCHASE):
    """Verificar si la última compra está dentro del timeframe relevante"""
    today = datetime.now(LOCAL_TZ).date()
    cutoff_date = today - timedelta(days=max_months * 30)
    return last_purchase_date >= cutoff_date

# ----------------------------------------------------- construcción DF —

def save_to_supabase(df):
    """Guardar DataFrame en Supabase"""
    try:
        supabase = get_supabase_client()
        
        if df.empty:
            print("⚠️  DataFrame vacío, no hay nada que guardar")
            return
        
        records = df.to_dict('records')
        
        # Limpiar registros
        for record in records:
            if 'fecha_ultima_compra' in record:
                record['fecha_ultima_compra'] = record['fecha_ultima_compra'].isoformat()
            
            for key in ['created_at', 'updated_at', 'id']:
                if key in record:
                    del record[key]
        
        # Hacer UPSERT en lotes
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table("clients_last_purchase").upsert(batch, on_conflict="cliente_id").execute()
        
        print(f"✅ {len(records)} registros guardados en Supabase")
        
    except Exception as e:
        print(f"❌ Error guardando en Supabase: {e}")
        raise

def build_full_report(contacts, all_sales):
    """Construir reporte completo desde todas las ventas"""
    print("📊 Construyendo reporte completo...")
    
    # Calcular últimas compras
    last_purchases = get_last_purchases_from_sales(all_sales)
    
    today = datetime.now(LOCAL_TZ).date()
    rows = []
    filtered_out_count = 0
    
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
        
        # Filtrar por timeframe
        if not is_within_timeframe(last_dt):
            filtered_out_count += 1
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
        print(f"✅ Reporte completo creado: {len(rows)} clientes")
        print(f"🔍 Filtrados {filtered_out_count} clientes con más de {MAX_MONTHS_WITHOUT_PURCHASE} meses sin compras")
    else:
        print("⚠️  No hay clientes elegibles para el reporte")

def update_client_reports(contacts, new_sales):
    """Actualizar reportes de clientes (modo incremental)"""
    all_last_purchases = get_last_purchases_from_db()
    updated_clients = set(sale["client_id"] for sale in new_sales)
    
    if not updated_clients:
        print("ℹ️  No hay clientes para actualizar")
        return
    
    today = datetime.now(LOCAL_TZ).date()
    rows = []
    filtered_out_count = 0
    
    print(f"🔄 Recalculando reportes para {len(updated_clients)} clientes afectados...")
    
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
            filtered_out_count += 1
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
        print(f"✅ Actualizados {len(rows)} clientes")
        print(f"🔍 Filtrados {filtered_out_count} clientes con más de {MAX_MONTHS_WITHOUT_PURCHASE} meses sin compras")
    else:
        print("ℹ️  No hay clientes elegibles para actualizar")

# ---------------------------------------------------------------- main —

def main():
    parser = argparse.ArgumentParser(description="Reporte de Alegra con opción de rebuild")
    parser.add_argument("--rebuild", action="store_true", 
                       help="Hacer rebuild completo (truncate + reload)")
    
    args = parser.parse_args()
    
    if args.rebuild:
        print("🔄 MODO REBUILD COMPLETO - Truncando y recargando todas las tablas")
        
        # Confirmar acción
        confirm = input("⚠️  Esto eliminará TODOS los datos existentes. ¿Continuar? (y/N): ")
        if confirm.lower() != 'y':
            print("❌ Operación cancelada")
            return
        
        # Truncar tablas
        truncate_tables()
        
        # Obtener contactos
        print("\n📞 Obteniendo contactos...")
        contacts = fetch_contacts()
        
        # Obtener TODAS las ventas
        print("\n🛒 Obteniendo todas las ventas...")
        all_sales = fetch_all_sales()
        
        # Guardar todas las ventas
        print(f"\n💾 Guardando {len(all_sales)} ventas...")
        save_new_sales(all_sales)
        
        # Construir reporte completo
        print(f"\n📊 Construyendo reporte completo...")
        build_full_report(contacts, all_sales)
        
        # Guardar estado
        save_state(datetime.now(LOCAL_TZ).date())
        
        print(f"\n✅ Rebuild completo finalizado")
        print(f"   • {len(all_sales)} ventas procesadas")
        print(f"   • Filtro temporal: {MAX_MONTHS_WITHOUT_PURCHASE} meses máximo")
        
    else:
        print("🚀 Iniciando reporte incremental de Alegra...")
        print(f"📅 Filtrando clientes con máximo {MAX_MONTHS_WITHOUT_PURCHASE} meses sin compras")

        # Modo incremental (código original)
        state = load_state()
        since = None
        if state["last_sync"]:
            since = datetime.fromisoformat(state["last_sync"]).date()
            print(f"📅 Procesando ventas desde: {since}")
        else:
            print("📅 Primera sincronización - procesando todas las ventas")
            since = date(2020, 1, 1)

        print("\n📞 Obteniendo contactos con ubicación...")
        contacts = fetch_contacts()

        print(f"\n🛒 Obteniendo ventas nuevas...")
        new_sales = fetch_new_sales(since)

        if new_sales:
            print(f"\n💾 Guardando {len(new_sales)} ventas nuevas...")
            save_new_sales(new_sales)

            time.sleep(2)

            print(f"\n📊 Actualizando reportes de clientes...")
            update_client_reports(contacts, new_sales)
        else:
            print("ℹ️  No hay ventas nuevas para procesar")

        save_state(datetime.now(LOCAL_TZ).date())

        print(f"\n✅ Proceso incremental completado")
        print(f"   • {len(new_sales)} ventas nuevas procesadas")
        print(f"   • Filtro temporal: {MAX_MONTHS_WITHOUT_PURCHASE} meses máximo")

if __name__ == "__main__":
    main()
