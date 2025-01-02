import zipfile
import os
import pandas as pd

# Ruta de la carpeta que contiene los archivos zip
carpeta_zip = 'C:/Users/Raknaros/Downloads/reportes_planilla'
reportes_planilla = None
# Iterar sobre los archivos en la carpeta
for archivo_zip in os.listdir(carpeta_zip):
    # Verificar si el archivo es un zip
    if archivo_zip.endswith('.zip'):
        # Ruta del archivo zip
        zip_file = os.path.join(carpeta_zip, archivo_zip)
        # Abrir el archivo zip
        with (zipfile.ZipFile(zip_file, 'r') as zip_ref):
            # Iterar sobre los archivos en el zip
            for file in zip_ref.namelist():
                # Verificar si el archivo es un txt
                if file[12:15] == 'TRA':
                    # Leer el archivo txt línea por línea
                    with zip_ref.open(file, 'r') as txt_file:
                        filas = []
                        for index, line in enumerate(txt_file):
                            if index == 2:
                                #RUC
                                ruc=(line.decode('latin-1').strip()[12:])
                            if index == 9:
                                #ENCABEZADO
                                encabezado=line.decode('latin-1').split('|')
                            if index >= 11:
                                if (line.decode('latin-1').strip()) != '':
                                    filas.append([e.strip() for e in line.decode('latin-1').split('|')])#

                        if reportes_planilla is None:
                            reportes_planilla = pd.DataFrame(filas, columns=encabezado)
                            reportes_planilla['reporte'] = file[12:15]
                            reportes_planilla['ruc'] = ruc
                        else:
                            df = pd.DataFrame(filas, columns=encabezado)
                            df['reporte'] = file[12:15]
                            df['ruc'] = ruc
                            reportes_planilla = pd.concat([reportes_planilla, df]).reset_index(drop=True)

print(reportes_planilla)


