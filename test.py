import os
import xml.etree.ElementTree as ET
from apscheduler.schedulers.blocking import BlockingScheduler
from time import sleep

"""
def job():
    print("I'm running")
    sleep(2)  # Simula una tarea que toma tiempo


scheduler = BlockingScheduler()

# Configura el ejecutor de hilos, si no lo especificas, usa uno por defecto con 10 hilos
scheduler.add_executor('threadpool', max_workers=20)

# Añade trabajos al scheduler
scheduler.add_job(job, 'interval', seconds=3, id='job1')
scheduler.add_job(job, 'interval', seconds=3, id='job2')

# Comienza la ejecución del scheduler
scheduler.start()
"""

# Directorio donde están los archivos XML
directorio = 'E:/TODOS LOS XML'

# Itera sobre los archivos en el directorio
for archivo in os.listdir(directorio):
    # Verifica si el archivo es un archivo XML
    if archivo.endswith('.xml'):
        # Lee el contenido del archivo XML
        with open(os.path.join(directorio, archivo), 'r') as f:
            primera_linea = f.readline()
            # Obtiene los primeros 50 caracteres de la primera línea
            primeros_50_caracteres = primera_linea[:50]
            print(f'Archivo: {archivo}')
            print(f'Primeros 50 caracteres: {primeros_50_caracteres}')
            print('---')
            # Carga el archivo XML
            tree = ET.parse('archivo.xml')
            root = tree.getroot()

            # Extrae los datos de la factura
            factura = {}
            factura['numero'] = root.find('.//{urn:oasis:names:specification:ubl:schema:xsd:Invoice-2}ID').text
            factura['fecha'] = root.find('.//{urn:oasis:names:specification:ubl:schema:xsd:Invoice-2}IssueDate').text
            factura['proveedor'] = root.find(
                './/{urn:oasis:names:specification:ubl:schema:xsd:Invoice-2}SupplierParty').find(
                './/{urn:oasis:names:specification:ubl:schema:xsd:Invoice-2}Name').text
