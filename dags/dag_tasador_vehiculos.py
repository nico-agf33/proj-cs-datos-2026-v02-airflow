"""
### Pipeline de Ingeniería de Datos - Tasación de Vehículos (Cosecha Mensual)
Grupo: 5K09-03
Propósito: Obtener el universo de usados cada 30 días para entrenamiento de modelo.
"""
from __future__ import annotations
import json
import zipfile
import os
import logging
import gzip 
import requests 
from pathlib import Path
import pendulum
import pandas as pd
from airflow.sdk import Param, Variable, dag, task, PokeReturnValue as ResultadoValidacion
from airflow.task.trigger_rule import TriggerRule

### logica de negocio
from autos import schema
from autos.collectors import carone, deruedas
from autos.normalize import _slug

log = logging.getLogger(__name__)

### config de directorios
DIR_SALIDA = Path("/usr/local/airflow/include/output")
DIR_BRONCE = DIR_SALIDA / "bronze"
DIR_PLATA = DIR_SALIDA / "silver"
DIR_FROZEN = Path("/usr/local/airflow/include/frozen")

VAR_ULTIMA_COSECHA = "autos_fecha_ultima_ingesta"

@dag(
    dag_id="tp1_5K09_03_autos_mensual",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Argentina/Buenos_Aires"),
    catchup=False,
    max_active_tasks=2,
    tags=["proyecto-integrador", "vehiculos"],
    params={
        "forzar_descarga": Param(False, type="boolean", title="Forzar recolección ahora"),
    },
)
def pipeline_vehiculos():

    @task
    def crear_carpetas_trabajo():
        ### crear la estructura de carpetas
        for carpeta in [DIR_BRONCE, DIR_PLATA, DIR_BRONCE / "deruedas"]:
            carpeta.mkdir(parents=True, exist_ok=True)
        log.info("directorios creados")
        return True

    ### sensor -> verificar que los portales respondan
    @task.sensor(poke_interval=60, timeout=1800, mode="reschedule", soft_fail=True)
    def validar_disponibilidad_fuentes():
        try:
            ### si se obtienen marcas, el sitio esta online
            m_carone = carone.get_available_brands()
            m_deruedas = deruedas.get_available_brands()
            if m_carone and m_deruedas:
                log.info("Fuentes en linea.")
                return ResultadoValidacion(is_done=True)
            return ResultadoValidacion(is_done=False)
        except Exception as e:
            log.warning("Fallo de conectividad: %s", e)
            return ResultadoValidacion(is_done=False)

    ### listado de marcas
    @task
    def obtener_marcas_actuales():
        marcas = deruedas.get_available_brands()
        if not marcas:
            raise ValueError("no se detectaron marcas para procesar.")
        return marcas

    ### branching -> ¿descarga nueva o capa frozen?
    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def elegir_ruta_datos(marcas_ok) -> str:
        if marcas_ok:
            return "chequear_refresh_mensual"
        return "usar_respaldo_congelado"

    ### short circuit -> ciclo de 30 dias
    @task.short_circuit
    def chequear_refresh_mensual(**context) -> bool:
        if context["params"]["forzar_descarga"]:
            return True
        ultima_fecha = Variable.get(VAR_ULTIMA_COSECHA, default=None)
        if not ultima_fecha:
            return True ### 1ra vez
        dias_transcurridos = (pendulum.now() - pendulum.parse(ultima_fecha)).days
        return dias_transcurridos >= 30

    ### capa bronce -> ingesta
    @task
    def cosecha_bronce_carone():
        ### el collector search() se detiene cuando la API no devuelve mas nada
        datos_api = carone.search() 
        ruta_archivo = DIR_BRONCE / "carone_raw.json"
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            json.dump(datos_api, f, ensure_ascii=False, indent=4)
        return str(ruta_archivo)

    @task(map_index_template="{{ task.op_kwargs['marca'] }}")
    def cosecha_bronce_deruedas(marca):
        ### HTML crudo 
        marca_dir = DIR_BRONCE / "deruedas" / f"marca={_slug(marca)}"
        marca_dir.mkdir(parents=True, exist_ok=True)
        
        saved_paths = []
        page = 1
        headers = {"User-Agent": "Mozilla/5.0"}
        
        while True:
            ### solicitar links de la pagina actual del buscador
            links = deruedas.fetch_search_page_links(marca, page)
            if not links:
                break

            for url in links:
                car_id = url.split("cod=")[-1]
                file_path = marca_dir / f"id_{car_id}.html.gz"
                
                ### si el archivo existe, no se vuelve a pedir (Append-only)
                if not file_path.exists():
                    try:
                        resp = requests.get(url, headers=headers, timeout=15)
                        resp.raise_for_status()
                        with gzip.open(file_path, "wt", encoding="utf-8") as f:
                            f.write(resp.text)
                        time.sleep(1.55) ### demora de proteccion
                    except:
                        continue
                
                saved_paths.append(str(file_path))
            
            page += 1
            if page > 50: break ### ''freno de seguridad''
            
        return saved_paths ### devolver lista de rutas de archivos HTML

    ### capa plata -> transformacion y dataset tidy
    @task
    def consolidar_capa_plata(json_carone, paths_dr_nested, **context):
        DIR_PLATA.mkdir(parents=True, exist_ok=True)
        registros_unificados = []
        
        ### cargar CarOne (JSON ya procesado por el collector)
        with open(json_carone, 'r') as f:
            registros_unificados.extend(json.load(f))
            
        ### cargar DeRuedas parseando los archivos HTML locales
        for path_list in paths_dr_nested:
            if not path_list: continue
            for file_path in path_list:
                try:
                    with gzip.open(file_path, "rt", encoding="utf-8") as f:
                        html_content = f.read()
                    
                    ### reconstruir la URL para que el ID sea consistente
                    car_id = Path(file_path).name.replace("id_", "").replace(".html.gz", "")
                    url_original = f"https://www.deruedas.com.ar/resul.asp?cod={car_id}"
                    
                    ### llamar al nuevo parser definido en el collector
                    data = deruedas.parse_html_to_dict(html_content, url_original)
                    if data:
                        registros_unificados.extend([data] if isinstance(data, dict) else data)
                except:
                    continue

        df = pd.DataFrame(registros_unificados)
        
        for col in schema.NUMERICAS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['grupo'] = Variable.get("grupo", default="5K09-03")
        df = df.drop_duplicates(subset=["id_publicacion"]).reset_index(drop=True)
        
        ds = context["ds"]
        ruta_final = DIR_PLATA / f"dataset_vehiculos_{ds}.csv"
        df.to_csv(ruta_final, index=False, encoding="utf-8")
        
        Variable.set(VAR_ULTIMA_COSECHA, ds)
        return str(ruta_final)

    ### capa frozen
    @task
    def usar_respaldo_congelado():
        ruta_semilla = DIR_FROZEN / "autos_semilla.csv"
        if ruta_semilla.exists():
            return str(ruta_semilla)
        raise FileNotFoundError("sin respaldo disponible en include/frozen/")

    ### validacion y entrega
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validar_calidad_dataset(ruta_csv):
        df = pd.read_csv(ruta_csv)
        meta_v = int(Variable.get("meta_volumen", default=9000))
        if len(df) < meta_v:
            raise ValueError(f"volumen insuficiente: {len(df)}/{meta_v}")
        return ruta_csv

    @task
    def generar_entregable_zip(csv_path: str, **context):
        dag_run = context["dag_run"]
        nombre_zip = f"tp1_5K09_03.zip"
        path_zip = DIR_SALIDA / nombre_zip
        with zipfile.ZipFile(path_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, arcname="dataset.csv")
            zipf.write(__file__, arcname="codigo_dag.py")
            
            ### incluir listado de archivos bronce
            path_bronce_txt = DIR_SALIDA / "bronce.txt"
            with open(path_bronce_txt, "w") as f:
                for file in DIR_BRONCE.rglob("*.gz"):
                    f.write(f"{file}\n")
            zipf.write(path_bronce_txt, arcname="bronce.txt")

            ### logs
            log_dir = f"/usr/local/airflow/logs/dag_id={dag_run.dag_id}/run_id={dag_run.run_id}"
            if os.path.exists(log_dir):
                for root, _, archivos in os.walk(log_dir):
                    for a in archivos:
                        ruta_abs = os.path.join(root, a)
                        zipf.write(ruta_abs, arcname=os.path.join("logs", os.path.relpath(ruta_abs, log_dir)))
        return str(path_zip)

    ### flujo del grafo
    setup = crear_carpetas_trabajo()
    fuentes_vivas = validar_disponibilidad_fuentes(setup)
    lista_marcas = obtener_marcas_actuales()
    camino = elegir_ruta_datos(lista_marcas)
    refresh = chequear_refresh_mensual()

    ### dependencias de ingesta
    c_raw = cosecha_bronce_carone()
    d_raw = cosecha_bronce_deruedas.expand(marca=lista_marcas, trigger=[setup])
    plata = consolidar_capa_plata(c_raw, d_raw)
    congelado = usar_respaldo_congelado()

    ### flujo de cierre
    validado = validar_calidad_dataset(plata)
    generar_entregable_zip(validado)

    ### orquestacion de dependencias
    fuentes_vivas >> lista_marcas >> camino
    camino >> [refresh, congelado]
    refresh >> [c_raw, d_raw]

pipeline_vehiculos()