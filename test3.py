import pandas as pd


def analizar_txt_tabulado(ruta_archivo: str):
    """
    Lee un archivo de texto separado por tabulaciones y muestra un análisis
    completo de su contenido.

    Args:
      ruta_archivo: La ruta al archivo .txt que se va a leer.
    """
    try:
        # Paso 1: Leer el archivo usando pandas.
        # sep='\t' le dice a pandas que el separador es una tabulación.
        df = pd.read_csv(ruta_archivo, sep='\t')

        print(f"✅ Archivo '{ruta_archivo}' leído exitosamente.")
        print("-" * 40)

        # Paso 2: Mostrar un resumen de los datos y sus tipos.
        # .info() es perfecto para esto: muestra columnas, conteo de no nulos y Dtype (tipo de dato).
        print("Resumen y Tipos de Datos de las Columnas:")
        df.info()

        print("\n" + "-" * 40)

        # Paso 3: Mostrar TODAS las filas del archivo.
        # Usamos .to_string() para asegurar que pandas no acorte la salida.
        print("Contenido Completo del Archivo:")
        print(df.to_string())

        df.to_excel('prueba_ventas_quebarbaro.xlsx', index=False)
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{ruta_archivo}' no fue encontrado.")
    except Exception as e:
        print(f"❌ Ocurrió un error al procesar el archivo: {e}")


# --- Ejemplo de Uso ---
if __name__ == "__main__":
    # Crea un archivo de prueba para el ejemplo


    # Llama a la función con el nombre de tu archivo
    analizar_txt_tabulado("C:/Users/Raknaros/Downloads/quebarbaro_ventas.txt")