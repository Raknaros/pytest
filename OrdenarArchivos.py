import os
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

directorio = '/path/al/directorio'
extension = '.pdf'
archivos_encontrados = analizar_archivos(directorio, extension)
for archivo in archivos_encontrados:
    print(archivo)