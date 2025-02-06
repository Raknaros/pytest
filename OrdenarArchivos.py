import os
import shutil
import zipfile

import paramiko

"""

FICHA RUC = 'reporteec_ficharuc_{ruc}_{YYYYMMDDHHMMSS}'
RESOLUCION DE INTENDENCIA (INGRESO COMO RECAUDACION) = 'ridetrac_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'
RESOLUCION DE INTENDENCIA (LIBERACION DE FONDOS) = 'rilf_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'
RESOLUCION DE MULTA = 'rmgen_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'
CONSTANCIA DE NOTIFICACION = 'constancia_{YYYYMMDDHHMMSS}_%PORDEFINIR%_{numero_resolucion}_%PORDEFINIR%}'
RESOLUCION COACTIVA = 'rvalores_{ruc}_{numero_resolucion}_{YYYYMMDDHHMMSS}_%PORDEFINIR%'
COMUNICACION DE BAJA DE INSCRIPCION DE OFICIO DEFINITIVA (formulario 2660) = 'bod_%PORDEFINIR%_{ruc}_{numero_formulario}'
FACTURA PDF = 'PDF-DOC-{numero_serie}-{numero_correlativo}{ruc}'
FACTURA XML = 'FACTURA{numero_serie}-{numero_correlativo}{ruc}'
BOLETA DE VENTA PDF = 'PDF-BOLETA{numero_serie}-{numero_correlativo}{ruc}'
NOTA DE CREDITO PDF = 'PDF-NOTA_CREDITO{numero_serio}{numero_correlativo}{ruc}'
RECIBO POR HONORARIOS PDF = 'RHE{ruc}{numero_serie}{numero_correlativo}'
GUIA DE REMISION REMITENTE PDF = '{ruc}-{tipo_documento}-{numero_serie}-{numero_correlativo}'
REPORTE PLANILLA T REGISTRO ZIP = '{ruc}_{codigo_reporte}_{DDMMYYYY}
DETALLE DECLARACIONES Y PAGOS EXCEL = 'DetalleDeclaraciones_{ruc}_%PORDEFINIR%'


"""
def analizar_archivos(directorio, extension):
    archivos_encontrados = []
    for root, dirs, files in os.walk(directorio):
        for file in files:
            if file.endswith(extension):
                archivos_encontrados.append(os.path.join(root, file))
            if file.endswith('.zip'):
                zip_file = zipfile.ZipFile(os.path.join(root, file))
                for zip_info in zip_file.infolist():
                    if zip_info.filename.endswith(extension):
                        archivos_encontrados.append(os.path.join(root, file) + ':' + zip_info.filename)
    return archivos_encontrados


directorio = 'C:/Users/Raknaros/Desktop/pdf_sunat'
extension = '.pdf'
archivos_encontrados = analizar_archivos(directorio, extension)
for archivo in archivos_encontrados:
    print(archivo)


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
