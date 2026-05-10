import pdfkit
import base64
import os
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

salessystem_url = f"{os.getenv('DB_SALESSYSTEM_DIALECT')}://{os.getenv('DB_SALESSYSTEM_USER')}:{os.getenv('DB_SALESSYSTEM_PASSWORD')}@{os.getenv('DB_SALESSYSTEM_HOST')}:{os.getenv('DB_SALESSYSTEM_PORT')}/{os.getenv('DB_SALESSYSTEM_NAME')}"
engine = create_engine(salessystem_url)

def obtener_datos_pedido(codigo_pedido):
    """
    Extrae la cabecera y el detalle filtrados por un código de pedido específico.
    """

    # --- QUERY CABECERA ---
    # Colocamos el WHERE al final de la consulta principal
    # NOTA: Se usa %% en DATE_FORMAT para escapar el % y evitar que PyMySQL lo interprete como parámetro.
    query_cabecera = f"""
    WITH montos AS (
        -- Agrupamos para obtener el subtotal por factura (CUI)
        SELECT
            cod_pedido,
            cuo,
            SUM(cantidad * precio_unit) as subtotal_calculado
        FROM facturas
        WHERE cod_pedido = '{codigo_pedido}' -- Filtro dinámico
        GROUP BY cod_pedido, cuo
    )
    SELECT DISTINCT
        CONCAT(f.cod_pedido, f.cuo) as cui,
        f.alias as alias_proveedor,
        CONCAT(SUBSTRING(CONCAT(f.cod_pedido, f.cuo), 2)) as nro_cotizacion,
        DATE_FORMAT(f.emision - INTERVAL 3 DAY, '%%d/%%m/%%Y') as fecha,
        DATE_FORMAT(f.emision + INTERVAL 10 DAY, '%%d/%%m/%%Y') as fecha_vencimiento,
        c.nombre_razon as cliente,
        f.ruc as ruc, -- RUC del Cliente
        ROUND(m.subtotal_calculado, 2) as subtotal,
        ROUND(m.subtotal_calculado * 0.18, 2) as igv,
        ROUND(m.subtotal_calculado * 1.18, 2) as total,
        'INTERBANK' as banco_nombre,
        i.ibk as banco_cuenta,
        i.cci_ibk as banco_cci,
        p.nombre_razon as banco_titular,
        '' as observaciones
    FROM facturas f
    JOIN customers c ON f.ruc = c.ruc
    JOIN proveedores p ON f.alias = p.alias
    -- 💡 CORRECCIÓN: Unimos info con el RUC del proveedor (p), no del cliente (f)
    LEFT JOIN info i ON p.numero_documento = i.ruc
    JOIN montos m ON f.cod_pedido = m.cod_pedido AND f.cuo = m.cuo
    WHERE f.cod_pedido = '{codigo_pedido}';
    """

    # --- QUERY DETALLE ---
    # El WHERE aquí es directo sobre la tabla de ítems
    query_detalle = f"""
    SELECT 
        CONCAT(cod_pedido, cuo) as cui,
        descripcion,
        cantidad,
        precio_unit as precio,
        ROUND(cantidad * precio_unit, 2) as total_linea
    FROM facturas
    WHERE cod_pedido = '{codigo_pedido}'
    ORDER BY cui, descripcion;
    """

    # 2. Ejecutar y cargar en DataFrames
    df_cab = pd.read_sql(query_cabecera, engine)
    df_det = pd.read_sql(query_detalle, engine)

    return df_cab, df_det

def image_to_base64(path):
    if not os.path.exists(path): return ""
    with open(path, "rb") as f:
        return f"data:image/png;base64,{base64.b64encode(f.read()).decode('utf-8')}"


# Configuración de Identidad Visual por Emisor
CONFIG_EMISORES = {
    "THB": {
        "template_name": "cotizacion_thb.html",
        "logo_path": r"D:\pytest\images\logo_tbhelectric.png",
        "estilo": {
            "color_primario": "#303030",
            "fuente": "'Helvetica', sans-serif",
            "textura_fondo": ""
        }
    },
    "IMPULSAOE": {
        "template_name": "cotizacion_impulsaoe.html",
        "logo_path": r"D:\pytest\images\logoimpulsaofi.png",
        "estilo": {
            "color_primario": "#00E1D9",
            "fuente": "'Verdana', sans-serif",
            "textura_fondo": ""
        }
    },
    "GADCA": {
        "template_name": "cotizacion_gadca.html",
        "logo_path": r"D:\pytest\images\logo_gadca.png",
        "estilo": {
            "color_primario": "#2EED84",
            "fuente": "'Verdana', sans-serif",
            "textura_fondo": "https://www.transparenttextures.com/patterns/az-subtle.png"
        }
    },
    "CONASECELIZ": {
        "template_name": "cotizacion_conaseceliz.html",
        "logo_path": r"D:\pytest\images\logo_conaseceliz.png",
        "estilo": {
            "color_primario": "#7888D3",
            "fuente": "'Georgia', serif",
            "textura_fondo": "https://www.transparenttextures.com/patterns/az-subtle.png"
        }
    },
    "PROCONCACH": {
        "template_name": "cotizacion_proconcach.html",
        "logo_path": r"D:\pytest\images\logo_proconcach.png",
        "estilo": {
            "color_primario": "#F1ECE8",
            "fuente": "'Arial', sans-serif",
            "textura_fondo": "https://www.transparenttextures.com/patterns/ag-square.png"
        }
    }
}

env = Environment(loader=FileSystemLoader('templates'))
config_pdf = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
options = {'page-size': 'A4', 'encoding': "UTF-8", 'no-outline': None, 'enable-local-file-access': None}


def procesar_documentos(df_cabecera, df_detalle):
    for _, fila in df_cabecera.iterrows():
        proveedor_key = fila['alias_proveedor']
        conf = CONFIG_EMISORES.get(proveedor_key)

        if not conf:
            print(f"⚠️ Sin configuración para {proveedor_key}");
            continue

        # Los datos bancarios y montos ya vienen en la 'fila' del df_cabecera
        contexto = fila.to_dict()

        # Filtramos los ítems del detalle
        servicios_items = df_detalle[df_detalle['cui'] == fila['cui']].to_dict('records')

        # Unimos todo en el contexto para Jinja2
        contexto.update({
            "logo_base64": image_to_base64(conf['logo_path']),
            "servicios": servicios_items,
            "estilo": conf['estilo'],
            "total": fila.get('total', 0)  # Asegúrate que tu DF tenga la columna total
        })

        template = env.get_template(conf['template_name'])
        html_out = template.render(contexto)

        if not os.path.exists("salida"): os.makedirs("salida")
        nombre_salida = f"salida/{fila['cui']}_{fila['cliente'][:15]}.pdf"

        try:
            pdfkit.from_string(html_out, nombre_salida, configuration=config_pdf, options=options)
            print(f"✅ Generado: {nombre_salida}")
        except Exception as e:
            print(f"❌ Error en {fila['cui']}: {e}")


# --- SCRIPT DE PREVISUALIZACIÓN ---
def previsualizar_plantilla(nombre_plantilla, datos_prueba):
    template = env.get_template(nombre_plantilla)
    html_renderizado = template.render(datos_prueba)

    # Guardamos el resultado en un archivo que Chrome SÍ puede leer
    with open("preview_final.html", "w", encoding="utf-8") as f:
        f.write(html_renderizado)

    print("✅ Previsualización lista. Abre 'preview_final.html' en tu navegador.")


# Datos de ejemplo para que no salga vacío
datos_test = {
    "estilo": CONFIG_EMISORES["THB"]["estilo"],
    "logo_base64": image_to_base64(CONFIG_EMISORES["THB"]["logo_path"]),
    "nro_cotizacion": "TEST-001",
    "fecha": "20/01/2026",
    "cliente": "CLIENTE DE PRUEBA S.A.C.",
    "ruc": "20123456789",
    "servicios": [{"descripcion": "Servicio de prueba", "monto": 100.50}],
    "total": 100.50,
    "banco_nombre": "BCP",
    "banco_cuenta": "191-000-000",
    "banco_cci": "002-191-000",
    "banco_titular": "BCP"
}

#previsualizar_plantilla("factura_impulsa.html", datos_test)
# 1. Definir el pedido que quieres procesar
pedido_a_generar = "R134FECF4"

# 2. Obtener los DataFrames de la BD
df_cabecera, df_detalle = obtener_datos_pedido(pedido_a_generar)

# 3. Validar si hay datos antes de procesar
if not df_cabecera.empty:
    print(f"🚀 Iniciando generación de {len(df_cabecera)} documentos para el pedido {pedido_a_generar}...")

    # 4. Llamar a tu función de procesamiento
    # Esta función ya sabe qué plantilla usar según la columna 'alias_proveedor'
    procesar_documentos(df_cabecera, df_detalle)
else:
    print("❌ No se encontraron facturas para ese código de pedido")