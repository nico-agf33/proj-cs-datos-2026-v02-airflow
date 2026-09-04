import re
import unicodedata

def remove_accents(text: str) -> str:
    if not text: return ""
    text = str(text).lower().strip()
    return "".join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

def as_number(val) -> float:
    ### Precios y KM -> quitar puntos (miles) y cambiar coma por punto
    if val is None or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).replace('.', '').replace(',', '.').strip()
    match = re.search(r'(\d+(\.\d+)?)', s)
    return float(match.group(1)) if match else 0.0

def parse_tecnico(val) -> float:
    ### Motor, Consumo, HP -> mantener el punto decimal y cambiar coma por punto
    if val is None or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).replace(',', '.').strip()
    match = re.search(r'(\d+(\.\d+)?)', s)
    return float(match.group(1)) if match else 0.0

def parse_consumo(val) -> float:
    return parse_tecnico(val)

def parse_motor(val) -> float:
    ### 1600 -> 1.6 | 1.7 lts -> 1.7
    num = parse_tecnico(val)
    if num == 0: return 0.0
    if num > 100:
        return round(num / 1000, 1)
    return num

def clean_price_and_currency(text: str) -> tuple[float, str]:
    if not text: return 0.0, "ARS"
    t = text.upper()
    currency = "USD" if ("U$" in t or "USD" in t) else "ARS"
    return as_number(t), currency

def _slug(text: str) -> str:
    return remove_accents(text).replace(" ", "-")

def format_consumption_carone(val) -> float:
    return parse_tecnico(val)