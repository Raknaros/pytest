import math

import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def load_cotizaciones(ruta: str):
    engine = create_engine('mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
                           ':3306/salessystem')

    cotizaciones = pd.read_excel(os.path.join(ruta, 'importar.xlsx'), sheet_name='cotizaciones', date_format='%d/%m/%Y',
                                 dtype={'ruc': np.int64, 'cantidad': np.int32, 'precio_unit': np.float32},
                                 parse_dates=[3, 17, 19, 21, 23, ],
                                 na_values=' ')

    alias_sheets = {}

    for i in cotizaciones['alias'].unique():
        alias_sheets[i] = cotizaciones[cotizaciones['alias'] == i]



    return print(alias_sheets)

load_cotizaciones('D:/')