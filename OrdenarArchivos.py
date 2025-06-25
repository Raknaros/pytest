import io
import os
import shutil
import zipfile
from io import BytesIO
import re

import paramiko

#UBICAR TODOS LOS ARCHIVOS QUE COMIENCEN CON ALGUNA FRASE IDENTIFICADA Y COLOCARLOS EN UNA LISTA
#OBTENER EL RUC DE TODOS LOS DOCUMENTOS Y VERIFICAR CUALES AUN NO TIENEN CARPERA Y/O REGISTRO EN ENTIDADES O RELACIONADOS
#CREAR LAS CARPETAS FALTANTES (RUC, PERIODO, COMPRAS, VENTAS, RESOLUCIONES, ETC)
#SI ES RESOLUCION, ENVIAR A RESOLUCIONES
#SI ES REPORTE, ENVIAR A REPORTES
#MOVER TODOS DE UBICACION A LA UBICACION CONVENIENTE(CENTRAL-SERVER, EC2 O A ESTA LAPTOP

# Patrones regex para los nombres estructurados
patrones_estructurados = {
    "patron_guia_remision": re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(pdf|xml)$", re.IGNORECASE),
    "reporte_planilla_zip": re.compile(r"^\d{11}_[A-Z]+_\d{8}\.zip$", re.IGNORECASE),
    "declaraciones_pagos": re.compile(r"^DetalleDeclaraciones_(\d{11})_(\d{14})\.xlsx$", re.IGNORECASE),
    "ficha_ruc": re.compile(r"^reporteec_ficharuc_(\d{11})_(\d{14})\.pdf$", re.IGNORECASE),
    "ingreso_recaudacion": re.compile(r"^ridetrac_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.pdf$", re.IGNORECASE),
    "liberacion_fondos": re.compile(r"^rilf_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.pdf$", re.IGNORECASE),
    "multa": re.compile(r"^rmgen_(\d{11})_(\d{3})-(\d{3})-(\d{7})_(\d{14})_(\d{9})\.pdf$", re.IGNORECASE),
    "notificacion": re.compile(r"^constancia_(\d{14})_(\d{20})_(\d{13})_(\d{9})\.pdf$", re.IGNORECASE),
    "valores": re.compile(r"^rvalores_(\d{11})_([A-Z0-9]{12,17})_(\d{14})_(\d{9})\.pdf$", re.IGNORECASE),
    "coactiva": re.compile(r"^recgen_(\d{11})_(\d{13})_(\d{14})_(\d{9})\.pdf$", re.IGNORECASE),
    "baja_oficio": re.compile(r"^bod_(\d{6})_(\d{11})_(\d{4})\.pdf$", re.IGNORECASE),
    "factura_pdf": re.compile(r"^PDF-DOC-([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.pdf$", re.IGNORECASE),
    "factura_xml": re.compile(r"^FACTURA([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
    "boleta_pdf": re.compile(r"^PDF-BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.pdf$", re.IGNORECASE),
    "boleta_xml": re.compile(r"^BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
    "credito_pdf": re.compile(r"^PDF-NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.pdf$", re.IGNORECASE),
    "credito_xml": re.compile(r"^NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
    "debito_pdf": re.compile(r"^PDF-NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.pdf$", re.IGNORECASE),
    "debito_xml": re.compile(r"^NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
    "recibo_pdf": re.compile(r"^RHE(\d{11})([A-Z0-9]{4})(\d{1,8})\.pdf$", re.IGNORECASE),
    "recibo_xml": re.compile(r"^RHE(\d{11})(\d{1,8})\.xml$", re.IGNORECASE),
}


def analizar_zip(zip_file, path_origen, archivos_encontrados):
    for zip_info in zip_file.infolist():
        if zip_info.is_dir():
            continue

        filename = os.path.basename(zip_info.filename)

        # Coincidencia por prefijo o patrón estructurado
        if any(patron.match(filename) for patron in patrones_estructurados.values()):
            archivos_encontrados.append(f"{path_origen}:{zip_info.filename}")

        # Si hay otro ZIP dentro, procesarlo recursivamente
        if filename.endswith(".zip"):
            if any(p.match(filename) for p in patrones_estructurados.values()):
                continue
            with zip_file.open(zip_info) as nested_file:
                nested_bytes = nested_file.read()
                with zipfile.ZipFile(io.BytesIO(nested_bytes)) as nested_zip:
                    nested_path = f"{path_origen}:{zip_info.filename}"
                    analizar_zip(nested_zip, nested_path, archivos_encontrados)


def analizar_archivos(directorio):
    archivos_encontrados = []

    for root, dirs, files in os.walk(directorio):
        for file in files:
            ruta_completa = os.path.join(root, file)

            # Archivos en disco
            if any(patron.match(file) for patron in patrones_estructurados.values()):
                archivos_encontrados.append(ruta_completa)

            # ZIPs en disco
            if file.endswith(".zip"):
                if any(p.match(file) for p in patrones_estructurados.values()):
                    continue
                try:
                    with zipfile.ZipFile(ruta_completa) as zip_file:
                        analizar_zip(zip_file, ruta_completa, archivos_encontrados)
                except zipfile.BadZipFile:
                    print(f"⚠️ Archivo ZIP inválido: {ruta_completa}")
                    continue

    return archivos_encontrados


# EJEMPLO DE USO
directorio = r"C:\Users\Raknaros\Desktop\pdf_sunat"
archivos_encontrados = analizar_archivos(directorio)

"""
def mover_archivos(archivos_encontrados, directorio_base, palabras_clave):
    for archivo in archivos_encontrados:
        for palabra in palabras_clave:
            if palabra in archivo:
                # Crea el directorio destino dinámico
                directorio_destino = os.path.join(directorio_base, palabra)
                if not os.path.exists(directorio_destino):
                    os.makedirs(directorio_destino)
                # Mueve el archivo al directorio correspondiente
                if ':' in archivo:
                    # Es un archivo dentro de un zip
                    zip_file_path, zip_file_name = archivo.split(':')
                    zip_file = zipfile.ZipFile(zip_file_path)
                    zip_file.extract(zip_file_name, directorio_destino)
                    # Mueve el zip al directorio correspondiente
                    shutil.move(zip_file_path, directorio_destino)
                else:
                    shutil.move(archivo, directorio_destino)
                break
"""


def parse_zip_path(full_path):
    """Separa una ruta en archivo físico y rutas internas de ZIPs (separadas por ':')."""
    full_path = full_path.replace('\\', '/')
    parts = full_path.split(':')

    if len(parts[0]) == 1 and parts[1].startswith('/'):
        drive = parts[0]
        rest = ':'.join(parts[1:]).lstrip('/')
        rest_parts = rest.split(':')
        base_path = f"{drive}:/{rest_parts[0]}"
        inner_paths = rest_parts[1:] if len(rest_parts) > 1 else []
    else:
        base_path = parts[0]
        inner_paths = parts[1:] if len(parts) > 1 else []

    base_path = base_path.replace('/', os.sep)
    print(f"Procesando ruta: {full_path}")
    print(f"base_path: {base_path}")
    print(f"inner_paths: {inner_paths}")
    return base_path, inner_paths


def extract_file_from_zip(zip_path, inner_path):
    """Extrae un archivo específico de un ZIP (o ZIP anidado)."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            if len(inner_path) == 1:
                return zf.read(inner_path[0])
            nested_zip_data = zf.read(inner_path[0])
            nested_zip_file = io.BytesIO(nested_zip_data)
            return extract_file_from_zip(nested_zip_file, inner_path[1:])
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} no es un archivo ZIP válido")
        raise
    except KeyError:
        print(f"Error: No se encontró {inner_path[0]} en {zip_path}")
        raise


def verify_zip_integrity(zip_path, expected_files):
    """Verifica que el ZIP contenga los archivos esperados."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zip_contents = zf.namelist()
            missing = [f for f in expected_files if f not in zip_contents]
            if missing:
                print(f"Advertencia: Archivos faltantes en el ZIP: {missing}")
                return False
            return True
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} no es un ZIP válido")
        return False


def delete_processed_file(file_path):
    """Borra un archivo del sistema de archivos si existe."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Eliminado: {file_path}")
        else:
            print(f"No se pudo eliminar, archivo no encontrado: {file_path}")
    except Exception as e:
        print(f"Error al eliminar {file_path}: {e}")


def create_final_zip(file_paths, output_zip_path, delete_zip_containers=False):
    """Crea un ZIP final y elimina los archivos procesados."""
    # Crear el directorio padre si no existe
    output_dir = os.path.dirname(output_zip_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Creado directorio: {output_dir}")

    included_names = set()
    processed_files = set()
    expected_files = set()

    with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as final_zip:
        for full_path in file_paths:
            base_path, inner_paths = parse_zip_path(full_path)
            filename = os.path.basename(inner_paths[-1] if inner_paths else base_path)

            if filename in included_names:
                print(f"Ignorado (ya incluido): {filename} en {full_path}")
                continue

            try:
                if not inner_paths:
                    if os.path.exists(base_path):
                        with open(base_path, 'rb') as f:
                            final_zip.writestr(filename, f.read())
                        included_names.add(filename)
                        expected_files.add(filename)
                        processed_files.add(base_path)
                        print(f"Agregado: {filename}")
                    else:
                        print(f"Archivo no encontrado: {base_path}")
                else:
                    zip_path = base_path
                    if os.path.exists(zip_path):
                        file_data = extract_file_from_zip(zip_path, inner_paths)
                        final_zip.writestr(filename, file_data)
                        included_names.add(filename)
                        expected_files.add(filename)
                        if delete_zip_containers:
                            processed_files.add(zip_path)
                        print(f"Agregado desde ZIP: {filename}")
                    else:
                        print(f"ZIP no encontrado: {zip_path}")
            except Exception as e:
                print(f"Error procesando {full_path}: {e}")

    # Verificar la integridad del ZIP
    if verify_zip_integrity(output_zip_path, expected_files):
        for file_path in processed_files:
            delete_processed_file(file_path)
    else:
        print("No se eliminaron archivos debido a problemas con el ZIP final")


output_zip_path = r"C:\Users\Raknaros\Desktop\pdf_sunat\archivos_comprimidos.zip"

create_final_zip(archivos_encontrados, output_zip_path, delete_zip_containers=False)
