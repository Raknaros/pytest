import pandas as pd
import os
from sqlalchemy import create_engine
import zipfile
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# .assign(ruc=i[0:11])
def load_f0621(carpeta: str):
    set_f0621 = pd.DataFrame(
        columns=['ruc', 'periodo_tributario', 'numero_orden', 'fecha_presentacion', '_100', '_101', '_102',
                 '_103', '_160', '_161', '_162', '_163', '_106', '_127', '_105', '_109', '_112', '_107', '_108', '_110',
                 '_111', '_113', '_114', '_115', '_116', '_117', '_119', '_120', '_122', '_172', '_169', '_173', '_340',
                 '_341', '_182', '_301', '_312', '_380', '_315', '_140', '_145', '_184', '_171', '_168', '_164', '_179',
                 '_176', '_165', '_185', '_187', '_188', '_353', '_351', '_352', '_347', '_342', '_343', '_344', '_302',
                 '_303', '_304', '_326', '_327', '_305', '_328', '_317', '_319', '_324', 'notas'
                 ])
    try:
        # Construir la URL de conexión desde variables de entorno
        db_url = f"{os.getenv('DB_WAREHOUSE_DIALECT')}://{os.getenv('DB_WAREHOUSE_USER')}:{os.getenv('DB_WAREHOUSE_PASSWORD')}@{os.getenv('DB_WAREHOUSE_HOST')}:{os.getenv('DB_WAREHOUSE_PORT')}/{os.getenv('DB_WAREHOUSEE_NAME')}"
        engine = create_engine(db_url)
        
        # Validar la conexión
        with engine.connect() as conn:
            pass
            
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None
    for i in list(filter(lambda x: (x[12:16] == '0621'), os.listdir(carpeta))):
        with (zipfile.ZipFile(carpeta + '/' + i, 'r') as zipf):
            with zipf.open(i[28:36] + i[0:11] + 'pdt621_casillas.csv') as archivo_csv:
                f0621 = pd.read_csv(archivo_csv, usecols=[6, 7], header=0).T.reset_index(drop=True)
                f0621.columns = f0621.iloc[0]
                f0621 = f0621[1:].add_prefix('_')
                f0621.rename(columns={'_2': 'ruc', '_7': 'periodo_tributario', '_13': 'fecha_presentacion'},
                             inplace=True)
                f0621 = f0621.assign(numero_orden=i[17:27], notas=','.join(
                    [f'{clave}:{valor.replace('.00', '')}' for clave, valor in
                     f0621[f0621.columns.difference(set_f0621.columns)].iloc[0].to_dict().items()]))
        set_f0621 = pd.concat([set_f0621, f0621[set_f0621.columns.intersection(f0621.columns)]], ignore_index=True)
    included = [col for col in set_f0621.columns if
                col not in ['ruc', 'periodo_tributario', 'numero_orden', 'fecha_presentacion', 'notas']]
    set_f0621[included] = set_f0621[included].apply(pd.to_numeric, errors='coerce')
    # pendiente verificar si en caso los que si son float(por ejemplo coeficiente 1.5) tambien cambian a int o
    # conservan su decimal.
    set_f0621[included] = set_f0621[included].astype(int, errors='ignore')
    return print(set_f0621.to_sql('_9', engine, if_exists='append', index=False, schema='acc'))  #


load_f0621('C:/Users/Raknaros/Desktop/temporal/0621')
