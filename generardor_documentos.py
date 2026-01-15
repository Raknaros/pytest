import pdfkit
from jinja2 import Environment, FileSystemLoader
import datetime

# 1. Configuración de la plantilla
env = Environment(loader=FileSystemLoader('templates'))
template = env.get_template('factura_impulsa.html')

# 2. Datos (Estos vendrían de tu SQL o variables)
datos = {
    "nro_cotizacion": "2026-001",
    "fecha": datetime.datetime.now().strftime("%d/%m/%Y"),
    "cliente": "Inversiones Tecnológicas S.A.C.",
    "ruc": "20601234567",
    "servicios": [
        {"descripcion": "Asesoría Contable Mensual", "monto": 500.00},
        {"descripcion": "Gestión de Planilla (PLAME)", "monto": 150.00},
        {"descripcion": "Auditoría Preventiva Anual", "monto": 1200.00}
    ]
}

# Calculamos el total automáticamente
datos["total"] = sum(item["monto"] for item in datos["servicios"])

# 3. Renderizar (Crear el HTML final con datos)
html_out = template.render(datos)

# 4. Convertir a PDF
# Indica aquí la ruta donde instalaste wkhtmltopdf
config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')

options = {
    'page-size': 'A4',
    'encoding': "UTF-8",
    'no-outline': None
}

nombre_archivo = f"Cotizacion_{datos['cliente'].replace(' ', '_')}.pdf"
pdfkit.from_string(html_out, nombre_archivo, configuration=config, options=options)

print(f"✅ Cotización generada: {nombre_archivo}")