import pandas as pd
from sqlalchemy import create_engine


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

warehouse = create_engine('postgresql://admindb:72656770@datawarehouse.cgvmexzrrsgs.us-east-1.rds.amazonaws.com'
                              ':5432/warehouse')

catalogo = pd.read_sql('SELECT * FROM catalogo', salessystem)

pre_detalle =pd.read_sql('SELECT * FROM pre_detalle ORDER BY fecha_emision DESC LIMIT 200', warehouse)

dfResultado = pd.merge(pre_detalle, catalogo, on='descripcion', how='left')

print(dfResultado.head(20))