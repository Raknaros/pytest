import pandas as pd

# Crear dos DataFrames de ejemplo
data1 = {
    'DateColumn': pd.to_datetime(['2021-01-01', '2021-02-01', '2021-03-01']),
    'ValueColumn': [100, 200, 300]
}
df1 = pd.DataFrame(data1)

data2 = {
    'Product': ['Apple', 'Banana', 'Orange'],
    'Quantity': [10, 20, 30]
}
df2 = pd.DataFrame(data2)

# Configurar el ExcelWriter con xlsxwriter
with pd.ExcelWriter('output_with_multiple_sheets.xlsx', engine='xlsxwriter') as writer:
    # Escribir el primer DataFrame en la primera hoja comenzando desde la segunda fila
    df1.to_excel(writer, sheet_name='Sheet1', startrow=1, index=False)

    # Obtener el objeto workbook y worksheet para la primera hoja
    workbook = writer.book
    worksheet1 = writer.sheets['Sheet1']

    # Escribir información personalizada en la primera fila de la primera hoja
    custom_info1 = 'Información personalizada en la primera fila de la primera hoja'
    worksheet1.write(0, 0, custom_info1)

    # Aplicar el formato de fecha a la columna datetime en la primera hoja
    date_format = workbook.add_format({'num_format': 'dd/mm/yyyy'})
    # Aplicar el formato a cada celda de la columna DateColumn
    for row_num in range(2, len(df1) + 2):  # Empieza desde la fila 2 (index 2) porque startrow=1
        worksheet1.write_datetime(row_num, 0, df1.at[row_num - 2, 'DateColumn'], date_format)

    # Aplicar formatos personalizados al encabezado en la primera hoja
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#D7E4BC',
        'border': 1
    })

    # Aplicar el formato al encabezado del DataFrame en la primera hoja
    for col_num, value in enumerate(df1.columns.values):
        worksheet1.write(1, col_num, value, header_format)

    # Escribir el segundo DataFrame en la segunda hoja comenzando desde la segunda fila
    df2.to_excel(writer, sheet_name='Sheet2', startrow=1, index=False)

    # Obtener el objeto worksheet para la segunda hoja
    worksheet2 = writer.sheets['Sheet2']

    # Escribir información personalizada en la primera fila de la segunda hoja
    custom_info2 = 'Información personalizada en la primera fila de la segunda hoja'
    worksheet2.write(0, 0, custom_info2)

    # Aplicar formatos personalizados al encabezado en la segunda hoja
    for col_num, value in enumerate(df2.columns.values):
        worksheet2.write(1, col_num, value, header_format)

print(
    "DataFrames guardados en 'output_with_multiple_sheets.xlsx' con información personalizada en la primera fila y formato de fecha dd/mm/yyyy en la primera hoja")
