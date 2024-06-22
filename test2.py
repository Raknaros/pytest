import pandas as pd

# Ejemplo de pivot table con multi index
data = {
    ('A', 'X'): [1, 2, 3],
    ('A', 'Y'): [4, 5, 6],
    ('B', 'X'): [7, 8, 9],
    ('B', 'Y'): [10, 11, 12]
}

index = pd.MultiIndex.from_tuples([('Grupo1', 'Item1'), ('Grupo1', 'Item2'), ('Grupo2', 'Item1')],
                                  names=['Grupo', 'Item'])

pivot_df = pd.DataFrame(data, index=index)

print("DataFrame pivot table:")
print(pivot_df)

# Resetear el índice para convertir la pivot table en un DataFrame regular
df = pivot_df.reset_index()

print("\nDataFrame con índice reseteado:")
print(df)