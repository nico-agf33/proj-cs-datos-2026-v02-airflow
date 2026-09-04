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

    ### limpiar valores numrricos generales (Precios, KM)
    ### el punto se interpreta como separador de miles

    if val is None or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    
    ### quitar puntos de unidades de mil y espacios, cambiar coma por punto decimal
    s = str(val).replace('.', '').replace(',', '.').strip()
    match = re.search(r'(\d+(\.\d+)?)', s)
    return float(match.group(1)) if match else 0.0

def parse_tecnico(val) -> float:

    ### limpiar valores tecnicos (Motor, Consumo, HP)
    ### el punto se mantiene como decimal y la coma se convierte en punto

    if val is None or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    
    ### no quitar el punto, solo cambiar coma por punto
    s = str(val).replace(',', '.').strip()
    match = re.search(r'(\d+(\.\d+)?)', s)
    if match:
        return float(match.group(1))
    return 0.0

def parse_motor(val) -> float:

    ### convertir de CC a Litros y mantener decimales
    ### '1600' -> 1.6
    ### '1.6 lts' -> 1.6

    num = parse_tecnico(val)
    if num == 0: return 0.0
    ### si es un valor de cilindrada en CC (ej: 1598, 1600, 1998)
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

def format_consumption_carone(val) -> str:
    ### extraer nro tecnico
    return parse_tecnico(val)