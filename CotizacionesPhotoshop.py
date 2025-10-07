import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()
db_connection_str = f"{os.getenv('DB_SALESSYSTEM_DIALECT')}://{os.getenv('DB_SALESSYSTEM_USER')}:{os.getenv('DB_SALESSYSTEM_PASSWORD')}@{os.getenv('DB_SALESSYSTEM_HOST')}:{os.getenv('DB_SALESSYSTEM_PORT')}/{os.getenv('DB_SALESSYSTEM_NAME')}"
cod_pedido_a_buscar = 'R134FECF4'
proveedor = 'IMPULSAOE'
nombre_tabla = 'facturas'  # <-- ASEGÚRATE DE PONER EL NOMBRE REAL DE TU TABLA AQUÍ

# --- LÓGICA ---
# 1. Conectar y obtener TODOS los datos para el pedido completo
db_engine = create_engine(db_connection_str)
sql_query = f"SELECT * FROM {nombre_tabla} WHERE cod_pedido = '{cod_pedido_a_buscar}' AND alias='{proveedor}';"
df_pedido_completo = pd.read_sql(sql_query, db_engine)

# Lista para almacenar los resultados finales de cada factura
lista_de_resultados = []

# 2. Identificar las facturas únicas (por 'cuo') y procesar cada una
facturas_unicas = df_pedido_completo['cuo'].unique()

for cuo in facturas_unicas:
    # Filtra el DataFrame para obtener solo los ítems de la factura actual
    df_factura_actual = df_pedido_completo[df_pedido_completo['cuo'] == cuo].copy()

    # --- INICIO DE LÓGICA DE PIVOTEO (aplicada por factura) ---

    # 2.1. Preparar los datos de la factura actual
    df_factura_actual['precio_unit'] = df_factura_actual['precio_unit'].round(2)  # <-- NUEVO: Redondear precio unitario
    df_factura_actual['total_item'] = (df_factura_actual['cantidad'] * df_factura_actual['precio_unit']).round(
        2)  # <-- MODIFICADO: Calcular y redondear total del ítem
    df_factura_actual = df_factura_actual.sort_values(by='id').reset_index(drop=True)
    df_factura_actual['numero_orden_item'] = range(1, len(df_factura_actual) + 1)

    # 2.2. Pivotear solo los ítems de esta factura
    df_pivot = df_factura_actual.pivot(
        index='cuo',
        columns='numero_orden_item',
        values=['descripcion', 'cantidad', 'precio_unit', 'total_item']
    )

    # 2.3. Aplanar y renombrar correctamente las columnas
    name_map = {'descripcion': 'desc', 'cantidad': 'cant', 'precio_unit': 'pre', 'total_item': 'total'}
    df_pivot.columns = [f"{name_map.get(col[0], col[0])}{col[1]}" for col in df_pivot.columns]

    # 2.4. Crear el DataFrame de resultado para esta factura
    resultado_factura = pd.DataFrame(index=[cuo])

    # 2.5. Añadir todas las columnas de ítems (hasta 8)
    for i in range(1, 9):
        for col_short_name in ['desc', 'cant', 'pre', 'total']:
            col_name = f'{col_short_name}{i}'
            if col_name in df_pivot.columns:
                resultado_factura[col_name] = df_pivot[col_name].iloc[0]
            else:
                resultado_factura[col_name] = '' if col_short_name == 'desc' else 0

    # 2.6. Añadir los datos generales y calculados para ESTA factura
    resultado_factura['numero'] = 'MANUAL-123'
    resultado_factura['fecha'] = (df_factura_actual['emision'].max() - pd.Timedelta(days=2))
    resultado_factura['vendedor'] = 'VENDEDOR A'
    resultado_factura['cliente'] = df_factura_actual['ruc'].iloc[0]

    # <-- NUEVO: Lógica de cálculo de totales con redondeo
    total_factura = df_factura_actual['total_item'].sum()
    subtotal_redondeado = round(total_factura / 1.18, 2)
    total_redondeado = round(total_factura, 2)

    resultado_factura['total'] = total_redondeado
    resultado_factura['subtotal'] = subtotal_redondeado
    resultado_factura['igv'] = round(total_redondeado - subtotal_redondeado,
                                     2)  # Calcular IGV a partir de los valores ya redondeados

    # 2.7. Añadir las columnas de visibilidad (con True/False)
    for i in range(1, 9):
        item_existe = pd.notna(resultado_factura[f'desc{i}'].iloc[0]) and resultado_factura[f'desc{i}'].iloc[0] != ''
        for col in ['desc', 'cant', 'pre', 'total']:
            resultado_factura[f'v_{col}{i}'] = bool(item_existe)

    # 2.8. Añadir esta factura procesada a nuestra lista de resultados
    lista_de_resultados.append(resultado_factura)

    # --- FIN DE LÓGICA DE PIVOTEO ---

# 3. Combinar todas las facturas en un solo DataFrame final
df_final = pd.concat(lista_de_resultados).reset_index(drop=True)

# 4. Reordenar las columnas a tu gusto para la visualización final
columnas_finales = ['numero', 'fecha', 'vendedor', 'cliente']
columnas_visibilidad = []
for i in range(1, 9):
    for prefix in ['', 'v_']:
        for col in ['desc', 'cant', 'pre', 'total']:
            full_col_name = f'{prefix}{col}{i}'
            if prefix == '':
                columnas_finales.append(full_col_name)
            else:
                columnas_visibilidad.append(full_col_name)
columnas_finales += ['subtotal', 'igv', 'total']
columnas_finales += columnas_visibilidad
df_final = df_final[columnas_finales]

# --- SALIDA ---
# Convertir el DataFrame a un string separado por tabuladores e imprimirlo en la consola
# 'sep=\t' especifica que el separador es un tabulador.
# 'index=False' evita que se imprima el índice numérico de Pandas (0, 1, 2...).
texto_para_copiar = df_final.to_csv(sep='\t', index=False)

print(texto_para_copiar)
