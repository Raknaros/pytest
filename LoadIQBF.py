import pandas as pd
import os

#pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#iqbf = pd.read_excel('C:\\Users\\Raknaros\\Desktop\\temporal\\DJRO_20548030529_202212.xls', sheet_name=3, skiprows=5,
#                     skipfooter=5, usecols='B:AG', date_format='%d/%m/%Y', parse_dates=[2, ], header=None,
#                     na_values=' ')
#iqbf.iloc[:, 3:27] = iqbf.iloc[:, 3:27].astype(object, errors='ignore')


# iqbf = iqbf.astype({'Equipo/Maquinaria': object, 'Placa Equipo Maquinaria ': object})
# iqbf['Fecha']=pd.to_datetime(iqbf['Fecha'], format='%d/%m/%Y')
#print(iqbf)
#columns=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18','19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32']
def load_iqbf(carpeta):
    set_iqbf = pd.DataFrame()
    for i in os.listdir(carpeta):
        iqbf = pd.read_excel('C:\\Users\\Raknaros\\Desktop\\temporal\\' + i, sheet_name=[0,3], skiprows=5,
                             skipfooter=5, usecols='B:AG', date_format='%d/%m/%Y', parse_dates=[2, ], header=None,
                             na_values=' ')
        set_iqbf = set_iqbf._append([iqbf[0].assign(ruc=i[5:16]),iqbf[3].assign(ruc=i[5:16])], ignore_index=True)
    set_iqbf.iloc[:, 3:27] = set_iqbf.iloc[:, 3:27].astype(object, errors='ignore')
    return print(set_iqbf)

load_iqbf('C:\\Users\\Raknaros\\Desktop\\temporal')
