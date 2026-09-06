import requests
from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from ..normalize import as_number, remove_accents, clean_price_and_currency, parse_motor, parse_tecnico

logger = logging.getLogger(__name__)

_BASE = "https://www.deruedas.com.ar"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_available_brands() -> list[str]:
    ### extraer todas las marcas disponibles para segmentar la busqueda
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

    ### descargar una pagina del buscador y devolver los links de los avisos
   ### funcion  usada por el DAG para saber que descargar

    params = f"segmento=0&marca={marca.replace(' ', '%20')}"
    url = f"{_BASE}/busCraw.asp?{params}&weNeed=divBusqueda&pag={page}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            if 'vendo/' in a['href']:
                full_url = a['href'] if a['href'].startswith("http") else _BASE + a['href']
                links.append(full_url)
        return list(dict.fromkeys(links))
    except Exception as e:
        logger.error(f"[deruedas] Error obteniendo links de página {page} para {marca}: {e}")
        return []

def parse_html_to_dict(html_content: str, url: str) -> dict | None:

    ### tomar HTML crudo (leido del disco) y transformarlo en un diccionario 'Plata'
    ### mantener nombres en español y tipos numericos

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

       ### Marca y Modelo exactos desde el bloque JS interno
        model_exact, make_exact = None, None
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string and "modelo:" in script.string:
                model_match = re.search(r"modelo:\s*'([^']+)'", script.string)
                marca_match = re.search(r"marca:\s*'([^']+)'", script.string)
                if model_match: model_exact = model_match.group(1)
                if marca_match: make_exact = marca_match.group(1)
                break 

        ### Precio y Moneda (desde la tabla visual)
        price_val, price_curr = 0.0, "ARS"
        for td in soup.find_all("td"):
            if "Precio:" in td.get_text():
                b_tag = td.find("b")
                if b_tag: 
                    price_val, price_curr = clean_price_and_currency(b_tag.get_text(strip=True))
                break

        ### Atributos tecnicos (cuadros box-destacado)
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
                ### el valor real suele estar en el tag <b>
                val = box.find("b").get_text(strip=True) if box.find("b") else content[-1]
                if label in mapping:
                    specs[mapping[label]] = val

        ### Metadatos Schema.org 
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
            ### normalizar floats tecnicos 
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