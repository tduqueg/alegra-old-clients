#!/usr/bin/env python3
"""
Versión con Supabase para almacenar los datos
"""

import os, json, requests, pandas as pd
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
    
    # MODO TESTING: solo obtener pocos registros
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
    max_items = int(os.getenv("TEST_MAX_ITEMS", "20")) if TEST_MODE else float('inf')
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
                    print(f"🧪 MODO TEST: Limitado a {max_items} registros")
                    return
                
            if len(batch) < 30:
                break
                
            params["start"] += 30
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            break

# ----------------------------------------------------------- manejo estado —

def load_state():
    """Cargar estado desde Supabase"""
    try:
        supabase = get_supabase_client()
        result = supabase.table("sync_state").select("*").limit(1).execute()
        
        if result.data:
            return result.data[0]
        return {"last_sync": None}
    except Exception as e:
        print(f"Error cargando estado: {e}")
        return {"last_sync": None}

def save_state(sync_date: date):
    """Guardar estado en Supabase"""
    try:
        supabase = get_supabase_client()
        
        # Eliminar registro anterior
        supabase.table("sync_state").delete().execute()
        
        # Insertar nuevo estado
        supabase.table("sync_state").insert({
            "last_sync": sync_date.isoformat(),
            "updated_at": datetime.now(LOCAL_TZ).isoformat()
        }).execute()
        
        print("✓ Estado guardado en Supabase")
    except Exception as e:
        print(f"Error guardando estado: {e}")

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
        cid = str(c["id"])
        
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
        client_id = str(inv["client"]["id"])
        sales.append((client_id, inv["date"]))
        invoice_count += 1
    
    print(f"✓ Obtenidas {invoice_count} facturas")
    
    # Remisiones
    remission_count = 0
    print("Obteniendo remisiones...")
    for rem in paginate("remissions", params=params):
        client_id = str(rem["client"]["id"])
        sales.append((client_id, rem["date"]))
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
    """Guardar DataFrame en Supabase (APPEND, no overwrite)"""
    try:
        supabase = get_supabase_client()
        
        # NO limpiar tabla - solo hacer UPSERT
        records = df.to_dict('records')
        
        # Convertir fechas a string ISO
        for record in records:
            if 'fecha_ultima_compra' in record:
                record['fecha_ultima_compra'] = record['fecha_ultima_compra'].isoformat()
        
        # Hacer UPSERT (insert o update si existe)
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            supabase.table("clients_last_purchase").upsert(batch, on_conflict="cliente_id").execute()
        
        print(f"✓ {len(records)} registros guardados/actualizados en Supabase")
        
    except Exception as e:
        print(f"Error guardando en Supabase: {e}")

def build_report(contacts, sales_list, df_prev=None):
    """Construye el reporte final"""
    
    # Calcular última fecha de compra por cliente
    last_purchase = {}
    for client_id, sale_date in sales_list:
        if client_id not in last_purchase or sale_date > last_purchase[client_id]:
            last_purchase[client_id] = sale_date

    today = datetime.now(LOCAL_TZ).date()
    rows = []
    
    # DEBUG: Contador para entender qué está pasando
    total_clients_with_sales = len(last_purchase)
    clients_in_contacts = 0
    clients_with_category = 0
    
    print(f"🔍 DEBUG: {total_clients_with_sales} clientes con ventas")
    
    # Procesar solo clientes que tienen ventas y pertenecen a categorías relevantes
    for client_id, last_date in last_purchase.items():
        if client_id not in contacts:
            continue
        
        clients_in_contacts += 1
        contact_info = contacts[client_id]
        price_id = contact_info["price_id"]
        categoria = category_from_price(price_id)
        
        # DEBUG: Mostrar algunos ejemplos
        if clients_in_contacts <= 5:  # Solo los primeros 5 para no saturar
            print(f"🔍 Cliente {client_id}: price_id={price_id}, categoria={categoria}")
        
        # Solo incluir Distribuidores y Mayoristas
        if not categoria:
            continue
            
        clients_with_category += 1
        last_dt = datetime.fromisoformat(last_date).date()
        rows.append({
            "cliente_id": client_id,
            "cliente_nombre": contact_info["name"],
            "cliente_email": contact_info["email"],
            "categoria": categoria,
            "lista_precio_id": price_id,
            "fecha_ultima_compra": last_dt,
            "dias_sin_compra": (today - last_dt).days,
        })
    
    print(f"🔍 DEBUG: {clients_in_contacts} clientes encontrados en contactos")
    print(f"🔍 DEBUG: {clients_with_category} clientes con categoría válida")
    print(f"🔍 DEBUG: {len(rows)} filas creadas para el reporte")

    df_new = pd.DataFrame(rows)
    
    # Combinar con datos previos si existen
    if df_prev is not None and not df_prev.empty:
        # Agregar columnas faltantes al df previo si es necesario
        for col in df_new.columns:
            if col not in df_prev.columns:
                df_prev[col] = ""
        
        df = pd.concat([df_prev, df_new], ignore_index=True) \
               .drop_duplicates("cliente_id", keep="last")
    else:
        df = df_new
    
    # CORRECCIÓN: Verificar si el DataFrame está vacío antes de hacer sort_values
    if df.empty:
        print("⚠️  No se encontraron clientes de las categorías Distribuidores o Mayoristas")
        # Crear DataFrame vacío con las columnas esperadas
        df = pd.DataFrame(columns=[
            "cliente_id", "cliente_nombre", "cliente_email", 
            "categoria", "lista_precio_id", "fecha_ultima_compra", 
            "dias_sin_compra"
        ])
        return df
    
    return df.sort_values("dias_sin_compra", ascending=False)

# ---------------------------------------------------------------- main —

def main():
    print("🚀 Iniciando reporte de Alegra con Supabase...")
    
    # 1) cargar estado
    state = load_state()
    since = None
    if state["last_sync"]:
        since = datetime.fromisoformat(state["last_sync"]).date() + timedelta(days=1)
        print(f"📅 Sincronizando desde: {since}")
    else:
        print("📅 Primera sincronización completa")

    # 2) descargar data
    print("\n📞 Obteniendo contactos...")
    contacts = fetch_contacts()
    
    print(f"\n🛒 Obteniendo ventas...")
    sales_list = fetch_sales(since)

    # 3) construir reporte
    print(f"\n📊 Construyendo reporte...")
    df_prev = existing_data()
    report = build_report(contacts, sales_list, df_prev)
    
    # 4) guardar en Supabase
    save_to_supabase(report)
    
    # 5) persistir fecha de sincronización
    save_state(datetime.now(LOCAL_TZ).date())
    
    print(f"\n✅ Reporte actualizado")
    print(f"   • {len(report)} clientes en total")
    print(f"   • Distribuidores: {len(report[report['categoria'] == 'Distribuidores'])}")
    print(f"   • Mayoristas: {len(report[report['categoria'] == 'Mayoristas'])}")
    
    # Mostrar algunos stats
    if not report.empty:
        print(f"   • Cliente más antiguo sin compras: {report['dias_sin_compra'].max()} días")
        print(f"   • Cliente más reciente: {report['dias_sin_compra'].min()} días")

if __name__ == "__main__":
    main()
