import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_iqbf(carpeta: str):
    set_iqbf = pd.DataFrame()
    engine = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                           ':5432/warehouse')
    for i in os.listdir(carpeta):
        iqbf = pd.read_excel(carpeta + '\\' + i, sheet_name=[0, 3], skiprows=5,
                             skipfooter=5, usecols='B:AG', date_format='%d/%m/%Y', parse_dates=[2, ], header=None,
                             na_values=' ')
        set_iqbf = set_iqbf._append([iqbf[0].assign(ruc=i[5:16]), iqbf[3].assign(ruc=i[5:16])], ignore_index=True)
    set_iqbf.iloc[:, 3:27] = set_iqbf.iloc[:, 3:27].astype(object, errors='ignore')
    set_iqbf.rename(
        columns={1: 'nro_operacion', 2: 'fecha', 3: 'periodo_mensual', 4: 'establecimiento', 5: 'tipo_operacion',
                 6: 'actividad',
                 7: 'tipo_servicio', 8: 'transaccion', 9: 'transferente', 10: 'adquiriente', 11: 'datos_propietario',
                 12: 'datos_destinatario',
                 13: 'traslado_guia', 14: 'sustento', 15: 'guia', 16: 'nro_matricula_transporte',
                 17: 'nombre_transporte', 18: 'equipo_maquinaria',
                 19: 'placa_equipo_maquinaria', 20: 'codigo_artesano', 21: 'tipo_documento_precedente',
                 22: 'nro_documento_precedente',
                 23: 'relacionado_bien', 24: 'documento', 25: 'dam', 26: 'bien', 27: 'cantidad', 28: 'presentacion',
                 29: 'cantidad_presentacion', 30: 'merma', 31: 'cantidad_neta_total', 32: 'observaciones'},
        inplace=True)
    # set_iqbf.to_sql('iqbf', engine, if_exists='append', index=False,schema='acc')
    return print(set_iqbf.to_sql('iqbf', engine, if_exists='append', index=False, schema='acc'))


load_iqbf('C:\\Users\\Raknaros\\Desktop\\temporal')
