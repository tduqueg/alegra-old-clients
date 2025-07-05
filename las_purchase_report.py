#!/usr/bin/env python3
"""
Genera / actualiza clients_last_purchase.csv con la info de Alegra.

• Usa price_lists_config.json para saber qué listas de precios pertenecen
  a Distribuidores y a Mayoristas.
• Primera ejecución: recorre toda la historia.
• Ejecuciones siguientes: sólo lo nuevo (append).
"""

import os, json, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil import tz

# ------------------------------------------------------------------ config —

BASE = "https://api.alegra.com/api/v1"
LOCAL_TZ = tz.gettz("America/Bogota")
DATA_CSV   = Path("clients_last_purchase.csv")
STATE_JSON = Path("state.json")
CONFIG = json.load(open("price_lists_config.json", encoding="utf8"))

DISTRIBUTOR_SET = set(CONFIG["distributor_lists"])   # {"4", "3"}
MAYORISTA_SET   = set(CONFIG["mayorista_lists"])     # {"5", "2"}

# ----------------------------------------------------------- utilidades API —

def auth():
    return (
        os.getenv("ALEGRA_API_EMAIL"),
        os.getenv("ALEGRA_API_TOKEN"),
    )

def paginate(endpoint, params=None):
    """Yields each item of a paginated Alegra endpoint."""
    params = params or {}
    params.update({"limit": 30, "start": 0})
    while True:
        r = requests.get(f"{BASE}/{endpoint}", auth=auth(),
                         params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        for item in batch:
            yield item
        if len(batch) < 30:
            break
        params["start"] += 30

# ----------------------------------------------------------- manejo estado —

def load_state():
    if STATE_JSON.exists():
        return json.load(open(STATE_JSON))
    return {"last_sync": None}

def save_state(sync_date: date):
    STATE_JSON.write_text(json.dumps({"last_sync": sync_date.isoformat()}, indent=2))

def existing_df():
    if DATA_CSV.exists():
        return pd.read_csv(DATA_CSV, parse_dates=["fecha_ultima_compra"])
    return None

# ------------------------------------------------------------- extracción —

def fetch_contacts():
    contacts = {}
    for c in paginate("contacts"):
        cid = c["id"]
        price_id = str(c.get("priceList", {}).get("id"))
        contacts[cid] = price_id
    return contacts

def fetch_sales(since: date | None):
    params = {}
    if since:
        params["date[from]"] = since.isoformat()
    # Invoices
    for inv in paginate("invoices", params=params):
        yield inv["client"]["id"], inv["date"]
    # Remissions
    for rem in paginate("remissions", params=params):
        yield rem["client"]["id"], rem["date"]

# ----------------------------------------------------- categorización —

def category_from_price(price_id: str | None):
    if price_id in DISTRIBUTOR_SET:
        return "Distribuidores"
    if price_id in MAYORISTA_SET:
        return "Mayoristas"
    return None

# ----------------------------------------------------- construcción DF —

def build_report(contacts, sales_iter, df_prev=None):
    # última fecha de compra por cliente
    last = {}
    for cid, d in sales_iter:
        if cid not in last or d > last[cid]:
            last[cid] = d

    today = datetime.now(LOCAL_TZ).date()
    rows = []
    for cid, last_date in last.items():
        price_id = contacts.get(cid)
        categoria = category_from_price(price_id)
        if not categoria:
            continue
        last_dt = datetime.fromisoformat(last_date).date()
        rows.append({
            "cliente_id": cid,
            "categoria": categoria,
            "lista_precio_id": price_id,
            "fecha_ultima_compra": last_dt,
            "dias_sin_compra": (today - last_dt).days,
        })

    df_new = pd.DataFrame(rows)
    if df_prev is not None and not df_prev.empty:
        df = pd.concat([df_prev, df_new], ignore_index=True)             \
               .drop_duplicates("cliente_id", keep="last")
    else:
        df = df_new
    return df.sort_values("dias_sin_compra", ascending=False)

# ---------------------------------------------------------------- main —

def main():
    # 1) cargar estado
    state = load_state()
    since = None
    if state["last_sync"]:
        since = datetime.fromisoformat(state["last_sync"]).date() + timedelta(days=1)

    # 2) descargar data
    contacts = fetch_contacts()
    sales_it = list(fetch_sales(since))

    # 3) reportar
    df_prev = existing_df()
    report = build_report(contacts, sales_it, df_prev)
    report.to_csv(DATA_CSV, index=False)

    # 4) persistir fecha de sincronización
    save_state(datetime.now(LOCAL_TZ).date())
    print(f"✓ Reporte actualizado — {len(report)} clientes")

if __name__ == "__main__":
    main()