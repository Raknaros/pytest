import zipfile
import os
import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
# Ruta de la carpeta que contiene los archivos zip
carpeta_zip = 'C:/Users/Raknaros/Downloads/reportes_planilla'
reportes_planilla = None
"""
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

reportes_planilla.to_excel('test_planillas.xlsx')
1 trabajador, 2 pensionista, 4 personal de terceros, 5 personal en formacion
cui=hex(ruc)+hex(categoria&tipo_documento&numero_documento)
"""
reporte = 'C:/Users/Raknaros/Downloads/test_planillas.xlsx'
reporte_df = pd.read_excel(reporte, parse_dates=['   Fec. Inicio   '], dtype={'numero_documento':object})#
reporte_df.drop(columns=['Column1'], inplace=True)
reporte_df.columns = ['tipo_documento', 'numero_documento', 'apellido_paterno', 'apellido_materno', 'nombres', 'fecha_inicio', 'tipo_trabajador', 'regimen_laboral', 'cat_ocu', 'ocupacion', 'nivel_educativo', 'discapacidad', 'sindicalizado', 'reg_acumulativo', 'maxima', 'horario_nocturno', 'situacion_especial', 'establecimiento', 'tipo_contrato', 'tipo_pago', 'periodicidad', 'entidad_financiera', 'nro_cuenta', 'remuneracion', 'situacion', 'reporte', 'ruc']
reporte_df['reporte'] = reporte_df['reporte'].replace({'TRA': '1', 'OTRO': 10})
reporte_df['tipo_documento'] = reporte_df['tipo_documento'].replace({'L.E / DNI': '01', 'OTRO': 10})
reporte_df['cui'] = reporte_df.apply(lambda row: str(hex(row['ruc']) + str(int(row['reporte'] + row['tipo_documento'] + str(row['numero_documento']))))[2:], axis=1)
datos_identificacion = reporte_df[['ruc', 'tipo_documento', 'numero_documento']]

print(reporte_df.head(5))
