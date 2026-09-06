import requests
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime
from ..normalize import as_number, remove_accents, clean_price_and_currency, parse_motor, parse_tecnico

logger = logging.getLogger(__name__)

_BASE = "https://www.deruedas.com.ar"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_available_brands() -> list[str]:
    url = f"{_BASE}/bus.asp?segmento=0"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        soup = BeautifulSoup(resp.text, 'html.parser')
        marcas = [i.get("value") for i in soup.select("#divModelosFancy input.fancyCheck") if i.get("value")]
        if not marcas:
            enlaces = soup.find_all("a", {"marcaVal": True})
            marcas = [a.get("marcaVal") for a in enlaces if a.get("marcaVal")]
        return list(dict.fromkeys([m.strip() for m in marcas if m]))
    except Exception as e:
        logger.error(f"[deruedas] Error descubriendo marcas: {e}")
        return []

def fetch_search_page_links(marca: str, page: int) -> list[str]:
    params = f"segmento=0&marca={marca.replace(' ', '%20')}"
    url = f"{_BASE}/busCraw.asp?{params}&weNeed=divBusqueda&pag={page}"
    ### pausa preventora de bloqueo antes de cada consulta al buscador
    time.sleep(2.0)
    resp = requests.get(url, headers=_HEADERS, timeout=20)
    resp.raise_for_status() 
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    links = [_BASE + a['href'] if a['href'].startswith('/') else a['href'] 
             for a in soup.find_all('a', href=True) if 'vendo/' in a['href']]
    return list(dict.fromkeys(links))

def parse_html_to_dict(html_content: str, url: str) -> dict | None:
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        model_exact, make_exact = None, None
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "modelo:" in script.string:
                model_match = re.search(r"modelo:\s*'([^']+)'", script.string)
                make_match = re.search(r"marca:\s*'([^']+)'", script.string)
                if model_match: model_exact = model_match.group(1)
                if make_match: make_exact = make_match.group(1)
                break 

        price_val, price_curr = 0.0, "ARS"
        for td in soup.find_all("td"):
            if "Precio:" in td.get_text():
                b_tag = td.find("b")
                if b_tag: price_val, price_curr = clean_price_and_currency(b_tag.get_text(strip=True))
                break

        mapping = {
            "motor": "motor_lt", 
            "potencia": "potencia_hp", 
            "transmision": "transmision", 
            "traccion": "traccion", 
            "combustible": "combustible", 
            "consumo prom.": "consumo_lt_100km"
        }
        specs = {}
        for box in soup.select(".box-destacado"):
            content = box.get_text(separator="|", strip=True).split("|")
            if len(content) >= 2:
                label = remove_accents(content[0])
                val = box.find("b").get_text(strip=True) if box.find("b") else content[-1]
                if label in mapping: specs[mapping[label]] = val

        def get_meta(prop):
            tag = soup.find("meta", itemprop=prop)
            return tag["content"] if tag else None

        return {
            "fuente": "deruedas",
            "id_publicacion": url.split("cod=")[-1],
            "marca": make_exact or get_meta("brand"),
            "modelo": model_exact or get_meta("model"),
            "version": soup.select_one(".titulo.resaltar span").get_text(strip=True) if soup.select_one(".titulo.resaltar span") else "",
            "fabricado_en": int(as_number(get_meta("modelDate") or 0)),
            "kilometraje": int(as_number(get_meta("mileageFromOdometer") or 0)),
            "precio": price_val,
            "moneda": price_curr,
            "motor_lt": parse_motor(specs.get("motor_lt")),
            "potencia_hp": parse_tecnico(specs.get("potencia_hp")),
            "transmision": specs.get("transmision"),
            "traccion": specs.get("traccion"),
            "combustible": specs.get("combustible"),
            "consumo_lt_100km": parse_tecnico(specs.get("consumo_lt_100km")),
            "ubicacion": get_meta("address"),
            "url": url,
            "fecha_ingesta": datetime.now().isoformat()
        }
    except Exception as e:
        logger.warning(f"Error parseando HTML de {url}: {e}")
        return None