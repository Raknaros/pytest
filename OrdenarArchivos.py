import io
import os
import shutil
import zipfile
from io import BytesIO
import re

import paramiko

"""

FICHA RUC = 'reporteec_ficharuc_{ruc}_{YYYYMMDDHHMMSS}'9_8_11_14=42
RESOLUCION DE INTENDENCIA (INGRESO COMO RECAUDACION) = 'ridetrac_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'8_11_13_14_9=59
RESOLUCION DE INTENDENCIA (LIBERACION DE FONDOS) = 'rilf_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'4_11_13_14_9=55
RESOLUCION DE MULTA = 'rmgen_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'5_11_3-3-7_14_9=58
CONSTANCIA DE NOTIFICACION = 'constancia_{YYYYMMDDHHMMSS}_%PORDEFINIR%_{numero_resolucion}_%PORDEFINIR%}'10_14_20_13_9=70
RESOLUCION COACTIVA = 'rvalores_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'8_11_X_14_9=46
COMUNICACION DE BAJA DE INSCRIPCION DE OFICIO DEFINITIVA (formulario 2660) = 'bod_%PORDEFINIR%_{ruc}_{numero_formulario}'3_6_11_4=27
FACTURA PDF = 'PDF-DOC-{numero_serie}-{numero_correlativo}{ruc}'=3-3-4-X-11=
FACTURA XML = 'FACTURA{numero_serie}-{numero_correlativo}{ruc}'
BOLETA DE VENTA PDF = 'PDF-BOLETA{numero_serie}-{numero_correlativo}{ruc}'
NOTA DE CREDITO PDF = 'PDF-NOTA_CREDITO{numero_serio}{numero_correlativo}{ruc}'
RECIBO POR HONORARIOS PDF = 'RHE{ruc}{numero_serie}{numero_correlativo}'
GUIA DE REMISION REMITENTE PDF = '{ruc}-{tipo_documento}-{numero_serie}-{numero_correlativo}'
REPORTE PLANILLA T REGISTRO ZIP = '{ruc}_{codigo_reporte}_{DDMMYYYY}
DETALLE DECLARACIONES Y PAGOS EXCEL = 'DetalleDeclaraciones_{ruc}_%PORDEFINIR%'
DETALLE DECLARACIONES Y PAGOS: DetalleDeclaraciones_(\d{11})_(\d{14})
FICHA RUC: reporteec_ficharuc_\d{11}_\d{14}\.pdf$
INGRESO COMO RECAUDACION: ridetrac_\d{11}_\d{13}_\d{14}\.pdf$
LIBERACION DE FONDOS: rilf_\d{11}_\d{13}_\d{14}_\d{9}\.pdf$
MULTA: rmgen_\d{11}_\d{13}_\d{14}_\d{9}\.pdf$
NOTIFICACION: constancia_\d{14}_\{20}_\d{13}_\d{9}\.pdf$
COACTIVA: rvalores_\d{11}_\d{13}_\d{14}_\d{9}\.pdf$
BAJA DE OFICIO: bod_\d{6}_\d{{11}_\d{4}\.pdf$
FACTURA PDF: PDF-DOC-([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.pdf$
BOLETA PDF:
BOLETA XML:
NOTA CREDITO PDF:
NOTA CREDITO XML:
NOTA DEBITO PDF:
NOTA DEBITO XML:
RECIBO PDF:
RECIBO XML:
"""

#UBICAR TODOS LOS ARCHIVOS QUE COMIENCEN CON ALGUNA FRASE IDENTIFICADA Y COLOCARLOS EN UNA LISTA
#OBTENER EL RUC DE TODOS LOS DOCUMENTOS Y VERIFICAR CUALES AUN NO TIENEN CARPERA Y/O REGISTRO EN ENTIDADES O RELACIONADOS
#CREAR LAS CARPETAS FALTANTES (RUC, PERIODO, COMPRAS, VENTAS, RESOLUCIONES, ETC)
#SI ES RESOLUCION, ENVIAR A RESOLUCIONES
#SI ES REPORTE, ENVIAR A REPORTES
#MOVER TODOS DE UBICACION A LA UBICACION CONVENIENTE(CENTRAL-SERVER, EC2 O A ESTA LAPTOP
inicios_sunat = ['reporteec_ficharuc_',
                 'ridetrac_',
                 'rilf_',
                 'rmgen_',
                 'constancia_',
                 'rvalores_',
                 'bod_',
                 'PDF-DOC',
                 'FACTURA',
                 'PDF-BOLETA',
                 'PDF-NOTA_CREDITO',
                 'RHE',
                 'DetalleDeclaraciones'
                 ]
# Patrones regex para los nombres estructurados
patrones_estructurados = {
    "patron_guia_remision": re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(pdf|xml)$", re.IGNORECASE),
    "reporte_planilla_zip": re.compile(r"^\d{11}_[A-Z]+_\d{8}\.zip$", re.IGNORECASE),
}

"""
def analizar_archivos(directorio):
    archivos_encontrados = []
    for root, dirs, files in os.walk(directorio):
        for file in files:
            for inicio in inicios_sunat:
                if file.endswith('.zip'):
                    zip_file = zipfile.ZipFile(os.path.join(root, file))
                    for zip_info in zip_file.infolist():
                        if zip_info.filename.startswith(inicio):  #cambiar a si comienza en
                            archivos_encontrados.append(os.path.join(root, file) + ':' + zip_info.filename)
                elif file.startswith(inicio):  #cambiar a si comienza en
                    archivos_encontrados.append(os.path.join(root, file))

    return archivos_encontrados
"""



def analizar_zip(zip_file, path_origen, archivos_encontrados):
    for zip_info in zip_file.infolist():
        if zip_info.is_dir():
            continue

        filename = os.path.basename(zip_info.filename)

        # Coincidencia por prefijo o patrón estructurado
        if any(filename.startswith(prefijo) for prefijo in inicios_sunat) or \
           any(patron.match(filename) for patron in patrones_estructurados.values()):
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
            if any(file.startswith(prefijo) for prefijo in inicios_sunat) or \
               any(patron.match(file) for patron in patrones_estructurados.values()):
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

for a in archivos_encontrados:
    print(a)


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


def agrupar_archivos_en_zip(lista_rutas, salida_zip):
    with zipfile.ZipFile(salida_zip, 'w', zipfile.ZIP_DEFLATED) as zip_salida:
        for ruta in lista_rutas:
            partes = ruta.split(':')
            archivo_base = partes[0]

            # Si es solo un archivo suelto (no está dentro de ZIPs)
            if len(partes) == 1:
                nombre_destino = os.path.basename(archivo_base)
                zip_salida.write(archivo_base, arcname=nombre_destino)
                continue

            # Caso ZIP o ZIP anidado
            with open(archivo_base, 'rb') as f:
                data = f.read()
            buffer = io.BytesIO(data)

            # Abrimos sucesivos niveles de ZIP
            zip_actual = zipfile.ZipFile(buffer)
            for i, parte in enumerate(partes[1:]):
                if i == len(partes[1:]) - 1:
                    # Es el archivo final a extraer
                    with zip_actual.open(parte) as archivo_final:
                        contenido = archivo_final.read()
                        nombre_destino = parte.replace('/', '_')
                        zip_salida.writestr(nombre_destino, contenido)
                else:
                    # Es un ZIP intermedio
                    with zip_actual.open(parte) as zip_intermedio:
                        nested_bytes = zip_intermedio.read()
                        zip_actual = zipfile.ZipFile(io.BytesIO(nested_bytes))

"""
# Ejemplo de uso
directorio = '/path/al/directorio'
extension = '.pdf'
palabras_clave = ['tipo1', 'tipo2', 'tipo3']
directorio_base = '/path/al/directorio/destino'

archivos_encontrados = analizar_archivos(directorio, extension)
mover_archivos(archivos_encontrados, directorio_base, palabras_clave)


def transferir_archivo(archivo_local, archivo_remoto, host, user, password):
    transport = paramiko.Transport((host, 22))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(archivo_local, archivo_remoto)
    transport.close()


# Ejemplo de uso:
transferir_archivo('archivo.txt', '/home/usuario/Documentos/archivo.txt', '192.168.1.100', 'usuario', 'contraseña')
"""
