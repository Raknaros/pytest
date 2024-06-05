import pandas as pd
from sqlalchemy import create_engine
import mysql.connector

data = {
    'col1': ['A', 'A', 'B', 'B', 'C', 'C', 'C'],
    'col2': [1, 2, 3, 4, 5, 6, 7],
    'col3': [10, 20, 30, 40, 50, 60, 70]
}
df = pd.DataFrame(data)

# Crear un diccionario para almacenar los resultados
result_dict = {}

# Obtener los valores únicos de la columna de interés
unique_values = df['col1'].unique()

# Iterar sobre cada valor único
for value in unique_values:
    # Filtrar el DataFrame para obtener el sub DataFrame
    sub_df = df[df['col1'] == value]

    # Iterar sobre las filas del sub DataFrame y agregar los valores de las columnas 2 y 3 a la lista
    for index, row in sub_df.iterrows():
        print(row['col2'], row['col3'])

