import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import numpy as np

# Create the initial DataFrame
data = {
    'grupo': ['A', 'A', 'B', 'B', 'C', 'C', 'D', 'D'],
    'fecha': ['2024-05-01', '2024-05-01', '2024-04-20', '2024-05-25', '2024-03-10', '2024-07-22', '2024-04-20', '2024-04-20'],
    'col1': [10, 15, 20, 25, 30, 35, 40, 45],
    'col2': [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5],
    'col3': [100, 200, 300, 400, 500, 600, 700, 800]
}

df = pd.DataFrame(data)

# Convert the 'fecha' column to datetime type
df['fecha'] = pd.to_datetime(df['fecha'])

# Define custom functions
def min_date_to_int(x):
    return int(x.min().strftime('%Y%m%d'))

def multiply_and_round_down(sub_df):
    # Replace missing values with 0 before multiplication
    sub_df[['col1']].fillna(0, inplace=True)
    sub_df['col2'].fillna(0, inplace=True)

    # Calculate the sum of element-wise products
    total_product = (sub_df[['col1']] * sub_df['col2']).sum()

    # Apply the IVA multiplier and rounding
    total_with_iva = np.floor(total_product * 1.18).astype(int)

    return total_with_iva

# Group by 'grupo' and apply custom functions
result = df.groupby('grupo').agg(
    fecha_min=('fecha', min_date_to_int),
    suma_multiplicacion=('grupo', multiply_and_round_down),
    max_col3=('col3', 'max')
).reset_index()

# Print the result
print(result)