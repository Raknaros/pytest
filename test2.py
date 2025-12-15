import pandas as pd
import os


def auditar_integridad(ruta_header, ruta_lines, ruta_salida):
    print("🔍 Iniciando auditoría de integridad...")

    # 1. Cargar los CSVs
    # dtype=str es importante para que no rompa el formato del CUI
    try:
        df_header = pd.read_csv(ruta_header, dtype=str)
        df_lines = pd.read_csv(ruta_lines, dtype=str)
    except FileNotFoundError as e:
        print(f"❌ Error: No se encuentra el archivo: {e.filename}")
        return

    # 2. Obtener los Sets de CUIs únicos
    cuis_cabecera = set(df_header['CUI'].unique())
    cuis_lineas = set(df_lines['CUI'].unique())

    print(f"📊 Cabeceras únicas encontradas: {len(cuis_cabecera)}")
    print(f"📊 Líneas únicas encontradas (por CUI): {len(cuis_lineas)}")

    # 3. Calcular la diferencia de conjuntos (A - B)
    # CUIs que están en Cabecera PERO NO en Líneas
    cuis_huerfanos = cuis_cabecera - cuis_lineas

    cantidad_huerfanos = len(cuis_huerfanos)

    if cantidad_huerfanos == 0:
        print("\n✅ ¡INTEGRIDAD CORRECTA! Todas las cabeceras tienen al menos una línea.")
    else:
        print(f"\n⚠️ ALERTA: Se encontraron {cantidad_huerfanos} cabeceras SIN líneas (Huérfanas).")

        # 4. Crear un DataFrame con los resultados y guardar
        df_resultado = pd.DataFrame({
            'cui_huerfano': list(cuis_huerfanos)
        })

        # Opcional: Traer el nombre del archivo original si está en el header
        if 'filename' in df_header.columns:
            # Hacemos merge para saber qué archivo XML causó el problema
            df_resultado = df_resultado.merge(
                df_header[['cui', 'filename']],
                left_on='cui_huerfano',
                right_on='cui',
                how='left'
            ).drop(columns=['cui'])

        archivo_salida = os.path.join(ruta_salida, 'reporte_cuis_sin_lineas.csv')
        df_resultado.to_csv(archivo_salida, index=False)
        print(f"📂 Reporte guardado en: {archivo_salida}")
        print("   Revisa estos XMLs, probablemente el parser de ítems falló en ellos.")


# --- EJECUCIÓN ---
if __name__ == "__main__":
    # Ajusta estas rutas a donde generaste tus reportes
    CARPETA_OUTPUT = r"D:\parser_sunat\output"

    # Busca los archivos más recientes o pon el nombre exacto
    FILE_HEADER = os.path.join(CARPETA_OUTPUT, "resultados_header_20251214_184628.csv")  # Pon el nombre exacto generado
    FILE_LINES = os.path.join(CARPETA_OUTPUT, "resultados_lines_20251214_184628.csv")  # Pon el nombre exacto generado

    auditar_integridad(FILE_HEADER, FILE_LINES, CARPETA_OUTPUT)


"""
RETIRAR LOS XML QUE YA ESTAN ANALIZADOS Y SUBIDOS A LA BASE DE DATOS
ventas_periodo = ventas[ventas['periodo_tributario'] == 202501]

ventas_periodo['nombre_xml'] = ventas_periodo.apply(lambda row: 'FACTURA' + row['numero_serie'] + '-' + str(row['numero_correlativo']) + str(row['ruc']) + '.xml', axis=1)


print(ventas_periodo['nombre_xml'].tolist())

for a in ventas_periodo['nombre_xml'].tolist():
    os.remove('E:/TODOS LOS XML/'+a)
"""
#VERIFICAR FACTURAS BANCARIZADAS POR PROVEEDOR
#VERIFICAR PERIODO DE LAS FACTURAS BANCARIZADAS POR PROVEEDOR
#VERIFICAR ADQUIRIENTES DE LAS FACTURAS BANCARIZADAS
#VERIFICAR PEDIDOS DE ESOS PROVEEDORES BANCARIZADOS
#VERIFICAR OTROS PEDIDOS
#EMITIR LOS MANIFIESTAMENTE PENDIENTES Y URGENTES
#VERIFICAR LOS OTROS PEDIDOS

#TODO VERIFICAR LA CONSISTENCIA DE LA DATA DE CIERRE DE MES EN LOS SIGUIENTES SENTIDOS:


#SOBRE LAS ENTIDADES/PROVEEDORES
#TODO CONSULTAR LISTA DE FACTURAS POR PERIODO TRIBUTARIO
#QUE CANTIDAD DE VENTAS TOTALES TIENE CADA ENTIDAD/PROVEEDOR
#TODO AGRUPAR POR ENTIDAD Y SUMAR TOTAL + IGV
#QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A PROVEEDORES TIPO 1
#TODO CONSULTA PROVEEDORES TIPO 1 Y 2 Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAS DE ESOS PROVEEDORES
#QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A PROVEEDORES TIPO 3
#TODO CONSULTAR PROVEEDORES TIPO 3 Y CONSULTA QUE FACTURAS CORRESPONDEN A COMPRAS DE ESOS PROVEEDORES
#QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A CUSTOMERS INTERNOS
#TODO CONSULTAR CUSTOMERS INTERNOS Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAR DE ESOS CUSTOMERS
#QUE CANTIDAD DE VENTAS CORRESPONDEN DE CADA ENTIDAD/PROVEEDOR A CUSTOMERS EXTERNOS
#TODO CONSULTAR CUSTOMERS EXTERNOS Y CONSULTAR QUE FACTURAS CORRESPONDEN A COMPRAR DE ESOS CUSTOMERS

#SOBRE LOS COMPROBANTES
#QUE CANTIDAD DE COMPROBANTES NO TIENEN GUIA
#TODO CONSULTA Y CONTAR COMPROBANTES QUE NO TENGAN ASOCIADA UNA GUIA
#QUE CANTIDAD DE COMPROBANTES TIENEN GUIA
#TODO CONSULTA Y CONTAR COMPROBANTES QUE TENGAN ASOCIADA UNA GUIA
#EXISTEN COMPROBANTES DE LA MISMA ENTIDAD QUE TENGAN LA MISMA GUIA ASIGNADA?
#TODO CONSULTAR SEGUN LA ENTIDAD/PROVEEDOR SI ALGUNA GUIA ASOCIADA SE REPITE Y COLOCAR ESTADO OBSERVADO GUIA REPETIDA


#SOBRE LOS PEDIDOS
#IDENTIFICAR LOS PEDIDOS EXISTENTES DEL PERIODO CON LA SUMA DE LOS TOTALES DE LAS FACTURAS EMITIDAS EN ORDEN DE FECHA
#TODO VERIFICAR PEDIDOS ENTREGADOS, SUMAR COMPROBANTES DE ESE ADQUIRIENTE SEGUN FECHA DE EMISION ASCENDENTE HASTA LLEGAR AL TOTAL DEL PEDIDO ENTREGADO
#EXCLUIR ESOS PEDIDOS YA EXISTENTES EN LA TABLA PEDIDOS DE LA AUTOGENERACION
#TODO FILTRAR ESAS FACTURAS DE LA LISTA PARA TRASLADAR Y AUTOGENERAR PEDIDOS
#COMPARAR LA INFORMACIO EXISTENTE DE LAS FACTURAS POR SI HUBIESE ALGUNA QUE AGREGAR DESDE WAREHOUSE A SALESSYSTEM
#TODO DETERMINAR QUE INFORMACION DE LA TABLA _5 WAREHOUSE ES NECESARIA TRASLADAR A FACTURAS PARA COMPLEMENTAR
#TRANSFERIR ENTRE BASES DE DATOS SOLO LAS FACTURAS QUE NO EXISTEN YA Y VERIFICAR SI HAY FORMA DE IDENTIFICARLAS POR TOTAL O POR ITEMS Y COLOCARLES EL NUMERO DE FACTURA Y GUIA SI FUESE NECESARIO
#TODO BUSCAR FORMA DE IDENTIFICAR _5 CON FACTURAS QUE TIENEN PENDIENTE EL NUMERO DE CORRELATIVO Y DE GUIA SI FUESE NECESARIO
#TRANSFERIR TAMBIEN LAS GUIAS
#TODO TRANSFERIR LAS GUIAS TAMBIEN

"""
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Token del bot
BOT_TOKEN = "tu_token_aqui"

# Comando para enviar el chat ID
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Tu chat ID es: {chat_id}")

def main():
    # Crear aplicación
    application = Application.builder().token(BOT_TOKEN).build()

    # Agregar manejador de comandos
    application.add_handler(CommandHandler("start", start))

    # Iniciar el bot
    application.run_polling()  # Usa run_polling directamente

if __name__ == "__main__":
    main()

"""
