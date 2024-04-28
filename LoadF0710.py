import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import zipfile

df1 = pd.DataFrame({
    'col1': ['a', 'b', 'c'],
    'col2': [1, 2, 3],
    'col3': ['x', 'y', 'z']
})

df2 = pd.DataFrame({
    'col1': ['c', 'e', 'f'],
    'col2': [4, 5, 6],
    'col4': ['u', 'v', 'w']
})

# Unir los dataframes
df_unido = pd.merge(df1, df2, how='outer')

print(df_unido)


dfs = [df1, df2, df3, ..., df50]  # reemplaza esto con tus dataframes

# Inicializa el dataframe final con el primer dataframe de la lista
df_final = dfs[0]

# Itera sobre los dataframes restantes y únelos al dataframe final
for df in dfs[1:]:
    df_final = pd.merge(df_final, df, on=['col1', 'col2'], how='outer')

print(df_final)