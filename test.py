import pandas as pd

# Crear un DataFrame vacío con las columnas col1 a col5
df1 = pd.DataFrame({'A': [1, 2, 3],
                    'B': [4, 5, 6],
                    'C': [7, 8, 9]})

# DataFrame 2
df2 = pd.DataFrame({'A': [10, 11, 12],
                    'D': [13, 14, 15],
                    'E': [16, 17, 18]})

# Obtener las columnas diferentes entre df1 y df2
columnas_diferentes = df2.columns.difference(df1.columns)

# Reindexar df2 para incluir las columnas de df1
df1 = df1.reindex(columns=df2.columns, fill_value=None)

# Seleccionar solo las columnas diferentes
df_diferentes = df1[columnas_diferentes]

print(df_diferentes)