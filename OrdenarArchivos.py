import os
import shutil
import zipfile

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