#!/usr/bin/env python3
"""
Versión con Supabase para almacenar los datos
"""

import os, json, requests, pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil import tz
import time
from supabase import create_client, Client

# ------------------------------------------------------------------ config —

BASE = "https://api.alegra.com/api/v1"
LOCAL_TZ = tz.gettz("America/Bogota")
CONFIG = json.load(open("price_lists_config.json", encoding="utf8"))

DISTRIBUTOR_SET = set(CONFIG["distributor_lists"])
MAYORISTA_SET = set(CONFIG["mayorista_lists"])

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
    
    # CORRECCIÓN: Diferentes límites para diferentes endpoints
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
    
    # En modo test, usar límites más altos para contactos
    if TEST_MODE:
        if endpoint == "contacts":
            max_items = int(os.getenv("TEST_MAX_CONTACTS", "500"))  # Más contactos
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

def save_state(sync_date: date):
    """Guardar estado en Supabase"""
    try:
        supabase = get_supabase_client()
        
        # CORRECCIÓN: Usar upsert en lugar de delete + insert
        # Esto evita el error de DELETE sin WHERE clause
        supabase.table("sync_state").upsert({
            "id": 1,  # Usar un ID fijo para el único registro de estado
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

def existing_data():
    """Cargar datos existentes desde Supabase"""
    try:
        supabase = get_supabase_client()
        result = supabase.table("clients_last_purchase").select("*").execute()
        
        if result.data:
            df = pd.DataFrame(result.data)
            df["fecha_ultima_compra"] = pd.to_datetime(df["fecha_ultima_compra"])
            return df
        return None
    except Exception as e:
        print(f"Error cargando datos existentes: {e}")
        return None

# ------------------------------------------------------------- extracción —

def fetch_contacts():
    """Obtiene todos los contactos con sus price lists"""
    contacts = {}
    contact_count = 0
    
    for c in paginate("contacts"):
        contact_count += 1
        cid = c["id"]  # Mantener como número
        
        price_list = c.get("priceList") or {}
        price_id = str(price_list.get("id", "")) if price_list.get("id") is not None else None
        
        contacts[cid] = {
            "price_id": price_id,
            "name": c.get("name", ""),
            "email": c.get("email", "")
        }
    
    print(f"✓ Obtenidos {contact_count} contactos")
    return contacts

def fetch_sales(since: date | None):
    """Obtiene todas las ventas (facturas y remisiones) desde una fecha"""
    sales = []
    
    params = {}
    if since:
        params["date[from]"] = since.isoformat()
    
    # Facturas
    invoice_count = 0
    print("Obteniendo facturas...")
    for inv in paginate("invoices", params=params):
        client_id = inv["client"]["id"]  # Mantener como número
        
        price_list_id = None
        if "priceList" in inv and inv["priceList"]:
            price_list_id = str(inv["priceList"]["id"])
        
        sales.append({
            "client_id": client_id,
            "date": inv["date"],
            "price_list_id": price_list_id,
            "type": "invoice"
        })
        invoice_count += 1
    
    print(f"✓ Obtenidas {invoice_count} facturas")
    
    # Remisiones
    remission_count = 0
    print("Obteniendo remisiones...")
    for rem in paginate("remissions", params=params):
        client_id = rem["client"]["id"]  # Mantener como número
        
        price_list_id = None
        if "priceList" in rem and rem["priceList"]:
            price_list_id = str(rem["priceList"]["id"])
        
        sales.append({
            "client_id": client_id,
            "date": rem["date"],
            "price_list_id": price_list_id,
            "type": "remission"
        })
        remission_count += 1
    
    print(f"✓ Obtenidas {remission_count} remisiones")
    print(f"✓ Total ventas: {len(sales)}")
    
    return sales
# ----------------------------------------------------- categorización —

def category_from_price(price_id: str | None):
    if price_id in DISTRIBUTOR_SET:
        return "Distribuidores"
    if price_id in MAYORISTA_SET:
        return "Mayoristas"
    return None

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
            # Convertir fechas a string ISO
            if 'fecha_ultima_compra' in record:
                record['fecha_ultima_compra'] = record['fecha_ultima_compra'].isoformat()
            
            # Remover campos problemáticos
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
        
def build_report(contacts, sales_list, df_prev=None):
    """Construye el reporte final"""
    
    # Calcular última fecha de compra por cliente
    last_purchase = {}
    for sale in sales_list:
        client_id = sale["client_id"]
        sale_date = sale["date"]
        price_list_id = sale["price_list_id"]
        
        if client_id not in last_purchase or sale_date > last_purchase[client_id]["date"]:
            last_purchase[client_id] = {
                "date": sale_date,
                "price_list_id": price_list_id
            }

    today = datetime.now(LOCAL_TZ).date()
    rows = []
    
    # Procesar solo clientes que tienen ventas
    for client_id, purchase_info in last_purchase.items():
        last_date = purchase_info["date"]
        sale_price_id = purchase_info["price_list_id"]
        
        # Obtener información del contacto
        contact_info = contacts.get(client_id, {})
        
        if not contact_info:
            print(f"⚠️  Cliente {client_id} no encontrado en contactos")
            continue
        
        # Prioridad al priceList de la venta, fallback al del contacto
        price_id = sale_price_id if sale_price_id else contact_info.get("price_id")
        categoria = category_from_price(price_id)
        
        # Solo incluir Distribuidores y Mayoristas
        if not categoria:
            continue
        
        last_dt = datetime.fromisoformat(last_date).date()
        rows.append({
            "cliente_id": str(client_id),
            "cliente_nombre": contact_info.get("name", ""),
            "cliente_email": contact_info.get("email", ""),
            "categoria": categoria,
            "lista_precio_id": price_id,
            "fecha_ultima_compra": last_dt,
            "dias_sin_compra": (today - last_dt).days,
        })

    df_new = pd.DataFrame(rows)
    
    # Combinar con datos previos si existen
    if df_prev is not None and not df_prev.empty:
        for col in df_new.columns:
            if col not in df_prev.columns:
                df_prev[col] = ""
        
        df = pd.concat([df_prev, df_new], ignore_index=True) \
               .drop_duplicates("cliente_id", keep="last")
    else:
        df = df_new
    
    if df.empty:
        print("⚠️  No se encontraron clientes de las categorías Distribuidores o Mayoristas")
        df = pd.DataFrame(columns=[
            "cliente_id", "cliente_nombre", "cliente_email", 
            "categoria", "lista_precio_id", "fecha_ultima_compra", 
            "dias_sin_compra"
        ])
        return df
    
    return df.sort_values("dias_sin_compra", ascending=False)
    
if __name__ == "__main__":
    main()
