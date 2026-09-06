"""
grupo 5K09-03
"""
from __future__ import annotations
import json
import zipfile
import os
import logging
import gzip
import requests
import time
import shutil 
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
        for carpeta in [DIR_BRONCE, DIR_PLATA]:
            carpeta.mkdir(parents=True, exist_ok=True)
        log.info("directorios creados")
        return True

    @task.sensor(poke_interval=60, timeout=1800, mode="reschedule", soft_fail=True)
    def validar_disponibilidad_fuentes(trigger):
        try:
            ### si se obtienen marcas, el sitio esta online
            m_carone = carone.get_available_brands()
            m_deruedas = deruedas.get_available_brands()
            if m_carone and m_deruedas:
                log.info("Fuentes en linea.")
                return ResultadoValidacion(is_done=True)
            return ResultadoValidacion(is_done=False)
        except Exception as e:
            log.warning("Fallodeconectividad: %s", e)
            return ResultadoValidacion(is_done=False)

    @task
    def obtener_marcas_actuales():
        marcas = deruedas.get_available_brands()
        if not marcas: raise ValueError("no se detectaron marcas.")
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

        ### capa bronce-> ingesta
    @task
    def cosecha_bronce_carone(trigger, **context): 
        ds = context["ds"]
                ### el collector search() se detiene cuando la API no devuelve mas nada
        datos_api = carone.search() 
        ruta_dir = DIR_BRONCE / f"batch={ds}"
        ruta_dir.mkdir(parents=True, exist_ok=True)
        ruta_archivo = ruta_dir / "carone_raw.json"
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            json.dump(datos_api, f, ensure_ascii=False, indent=4)
        return str(ruta_archivo)

    @task(
        map_index_template="{{ task.op_kwargs['marca'] }}",
        retries=3,                            
        retry_delay=pendulum.duration(minutes=10)
    )
    def cosecha_bronce_deruedas(marca, trigger, **context): 
        ds = context["ds"]
        marca_dir = DIR_BRONCE / f"batch={ds}" / "deruedas" / f"marca={_slug(marca)}"
        marca_dir.mkdir(parents=True, exist_ok=True)
        
        saved_paths = []
        page = 1
        headers = {"User-Agent": "Mozilla/5.0"}
        
        while True:
            links = deruedas.fetch_search_page_links(marca, page)
            if not links: break

            for url in links:
                car_id = url.split("cod=")[-1]
                file_name = f"id_{car_id}.html.gz"
                file_path = marca_dir / file_name
                
                ### buscar si el archivo existe en un batch anterior
                archivo_previo = None
                for p in DIR_BRONCE.glob(f"batch=*/deruedas/marca={_slug(marca)}/{file_name}"):
                    if p.exists() and p != file_path:
                        archivo_previo = p
                        break

                if archivo_previo:
                    ### Si existe, copiar al batch actual 
                    shutil.copy2(archivo_previo, file_path)
                    log.info(f"Reutilizando archivo previo para ID {car_id}")
                elif not file_path.exists():
                    ### descargar si no existe
                    try:
                        resp = requests.get(url, headers=headers, timeout=15)
                        resp.raise_for_status() 
                        with gzip.open(file_path, "wt", encoding="utf-8") as f:
                            f.write(resp.text)
                        time.sleep(1.55) 
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429: raise 
                        continue
                    except: continue
                
                saved_paths.append(str(file_path))
            page += 1 

        return saved_paths 

    @task
    def consolidar_capa_plata(json_carone, paths_dr_nested, **context):
        DIR_PLATA.mkdir(parents=True, exist_ok=True)
        registros_unificados = []
        
            ### procesar CarOne
        with open(json_carone, 'r') as f:
            registros_unificados.extend(json.load(f))
            
            ### procesar deRuedas leyendo HTML.gz del disco
        for path_list in paths_dr_nested:
            if not path_list: continue
            for file_path in path_list:
                try:
                    with gzip.open(file_path, "rt", encoding="utf-8") as f:
                        html_content = f.read()
                    car_id = Path(file_path).name.replace("id_", "").replace(".html.gz", "")
                    url_original = f"https://www.deruedas.com.ar/resul.asp?cod={car_id}"
                    data = deruedas.parse_html_to_dict(html_content, url_original)
                    if data: registros_unificados.append(data)
                except: continue

        df = pd.DataFrame(registros_unificados)
        for col in schema.NUMERICAS:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['grupo'] = Variable.get("grupo", default="5K09-03")
        df = df.drop_duplicates(subset=["id_publicacion"]).reset_index(drop=True)
        
        ds = context["ds"]
        ruta_final = DIR_PLATA / f"final_{ds}.csv"
        df.to_csv(ruta_final, index=False, encoding="utf-8")
        Variable.set(VAR_ULTIMA_COSECHA, ds)
        return str(ruta_final)

    ### capa frozen
    @task
    def usar_respaldo_congelado():
        ruta_semilla = DIR_FROZEN / "autos_semilla.csv"
        if ruta_semilla.exists(): return str(ruta_semilla)
        raise FileNotFoundError("sin respaldo disponible.")

    ### validaciones
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def validar_calidad_dataset(ruta_csv):
        df = pd.read_csv(ruta_csv)
        meta_v = int(Variable.get("meta_volumen", default=9000))
        
        reporte_calidad = {
            "1_volumen_suficiente": {"filas_encontradas": int(len(df)), "meta_minima": meta_v, "estado": "OK" if len(df) >= meta_v else "ERROR"},
            "2_clave_unica": {"is_unique": bool(df["id_publicacion"].is_unique), "estado": "OK" if df["id_publicacion"].is_unique else "ERROR"},
            "3_ancho_suficiente": {"columnas_encontradas": int(df.shape[1]), "estado": "OK" if df.shape[1] >= 5 else "ERROR"},
            "4_mezcla_tipos": df.dtypes.value_counts().astype(str).to_dict(),
            "5_nulos_por_columna": df.isna().mean().sort_values(ascending=False).to_dict(),
            "6_columnas_vacias": df.columns[df.isna().all()].tolist(),
        }

        path_calidad_json = DIR_SALIDA / "calidad.json"
        with open(path_calidad_json, "w", encoding="utf-8") as f:
            json.dump(reporte_calidad, f, indent=4, ensure_ascii=False)

        if reporte_calidad["1_volumen_suficiente"]["estado"] == "ERROR":
            raise ValueError(f"volumen insuficiente: {len(df)}/{meta_v}")
        if not reporte_calidad["2_clave_unica"]["is_unique"]:
            raise ValueError("Error: La clave id_publicacion tiene duplicados")
            
        log.info("Validación completada.")
        return ruta_csv

      ### generar el ZIP final incluyendo el reporte de calidad
    @task
    def generar_entregable_zip(csv_path: str, **context):
        import json, zipfile, pendulum
        dag_run = context["dag_run"]
        ds = context["ds"]
        grupo_id = Variable.get("grupo", default="5K09-03")
        nombre_zip = f"tp1_{_slug(grupo_id)}.zip"
        path_zip = DIR_SALIDA / nombre_zip
        
        df = pd.read_csv(csv_path)
            ###  manifiesto dinamico
        manifiesto = {
            "grupo": grupo_id,
            "integrantes": ["Garcia, Nicolas", "Via, Tomas", "Ramos, Ignacio", "Martin, Sergio", "Velasco, Victoria"],
            "fuentes": ["Carone (GraphQL)", "DeRuedas (Scraping)"],
            "filas": int(df.shape[0]),
            "columnas": int(df.shape[1]),
            "run_id": dag_run.run_id,
            "batch_fecha": ds,
            "generado_en": pendulum.now().to_iso8601_string()
        }
        path_mani = DIR_SALIDA / "manifiesto.json"
        with open(path_mani, "w") as f: json.dump(manifiesto, f, indent=4)

        path_bronce_txt = DIR_SALIDA / "bronce.txt"
        ruta_batch_actual = DIR_BRONCE / f"batch={ds}"
        with open(path_bronce_txt, "w") as f:
            if ruta_batch_actual.exists():
                for file in ruta_batch_actual.rglob("*.*"): f.write(f"{file}\n")

        with zipfile.ZipFile(path_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_path, arcname="dataset.csv")
            zipf.write(path_mani, arcname="manifiesto.json")
            zipf.write(DIR_SALIDA / "calidad.json", arcname="calidad.json")
            zipf.write(path_bronce_txt, arcname="bronce.txt")
            zipf.write(__file__, arcname="codigo_dag.py")
            
            log_dir = f"/usr/local/airflow/logs/dag_id={dag_run.dag_id}/run_id={dag_run.run_id}"
            if os.path.exists(log_dir):
                for root, _, archivos in os.walk(log_dir):
                    for a in archivos:
                        ruta_abs = os.path.join(root, a)
                        zipf.write(ruta_abs, arcname=os.path.join("logs", os.path.relpath(ruta_abs, log_dir)))
        return str(path_zip)

    @task
    def limpiar_historial_bronce(trigger):
        """Mantiene únicamente el batch actual y el anterior."""
        batches = sorted([d for d in DIR_BRONCE.iterdir() if d.is_dir() and d.name.startswith("batch=")])
        ### borrar el batch mas desactualizado
        if len(batches) > 2:
            a_borrar = batches[:-2]
            for folder in a_borrar:
                log.info(f"Limpiando batch antiguo: {folder.name}")
                shutil.rmtree(folder)
        return True

    ### grafo
    setup = crear_carpetas_trabajo()
    vivas = validar_disponibilidad_fuentes(setup)
    marcas = obtener_marcas_actuales()
    vivas >> marcas
    
    camino = elegir_ruta_datos(marcas)
    refresh = chequear_refresh_mensual()
    
    c_raw = cosecha_bronce_carone(setup) 
    d_raw = cosecha_bronce_deruedas.expand(marca=marcas, trigger=[setup]) 
    
    plata = consolidar_capa_plata(c_raw, d_raw)
    congelado = usar_respaldo_congelado()

    camino >> [refresh, congelado]
    refresh >> [c_raw, d_raw]
    
    zip_final = generar_entregable_zip(validar_calidad_dataset(plata))
    
    limpiar_historial_bronce(zip_final)

pipeline_vehiculos()