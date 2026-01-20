import pdfkit
import base64
import os
from jinja2 import Environment, FileSystemLoader

# Función auxiliar para el logo
def image_to_base64(path):
    if not os.path.exists(path): return ""
    with open(path, "rb") as f:
        return f"data:image/png;base64,{base64.b64encode(f.read()).decode('utf-8')}"

# DICCIONARIO MAESTRO: Aquí configuras cada emisor/alias
CONFIG_EMISORES = {
    "THB_ELECTRIC": {
        "template_name": "proforma_thb.html",
        "logo_path": r"D:\pytest\images\logo_tbhelectric.png",
        "datos_bancarios": {
            "titular": "TBH ELECTRIC S.A.C.",
            "bancos": [
                {"nombre": "BCP Soles", "cuenta": "191-XXX", "cci": "002-XXX"},
                {"nombre": "Interbank", "cuenta": "200-XXX", "cci": "003-XXX"}
            ]
        }
    },
    "LIBELAW": {
        "template_name": "factura_libelaw.html",
        "logo_path": r"D:\pytest\images\logo_libelaw.png",
        "datos_bancarios": {
            "titular": "LIBELAW ASESORES S.A.C.",
            "bancos": [
                {"nombre": "BBVA Soles", "cuenta": "0011-XXX", "cci": "011-XXX"}
            ]
        }
    }
}

#TODO DATAFRAMES df_cabecera y df_detalle

# 1. Preparar Entorno
env = Environment(loader=FileSystemLoader('templates'))
config_pdf = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
options = {'page-size': 'A4', 'encoding': "UTF-8", 'no-outline': None}


def procesar_documentos(df_cabecera, df_detalle):
    for _, fila in df_cabecera.iterrows():
        # A. Obtener configuración según el proveedor de la fila
        proveedor_key = fila['alias_proveedor']  # Ej: "THB_ELECTRIC"
        conf = CONFIG_EMISORES.get(proveedor_key)

        if not conf:
            print(f"⚠️ No hay configuración para {proveedor_key}. Saltando...");
            continue

        # B. Filtrar detalles (Servicios/Productos) para este CUI
        servicios_items = df_detalle[df_detalle['cui'] == fila['cui']].to_dict('records')

        # C. Construir Contexto de Datos
        contexto = fila.to_dict()  # Datos de la cabecera (cliente, nro_orden, etc)
        contexto.update({
            "logo_base64": image_to_base64(conf['logo_path']),
            "servicios": servicios_items,
            "bancos": conf['datos_bancarios']['bancos'],
            "titular_bancos": conf['datos_bancarios']['titular']
        })

        # D. Renderizado Dinámico
        template = env.get_template(conf['template_name'])
        html_out = template.render(contexto)

        # E. Generar PDF
        nombre_salida = f"salida/{fila['cui']}_{fila['cliente'][:15]}.pdf"
        pdfkit.from_string(html_out, nombre_salida, configuration=config_pdf, options=options)

        print(f"✅ Documento generado: {nombre_salida}")

# Solo queda llamar a la función con tus DFs
# procesar_documentos(df_cabecera, df_detalle)