import pandas as pd
from sqlalchemy import create_engine


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


salessystem = create_engine(
        'mysql+pymysql://admin:Giu72656770@sales-system.c988owwqmmkd.us-east-1.rds.amazonaws.com'
        ':3306/salessystem')

customers = pd.read_sql('select * from customers', salessystem)
#nuevos_customers = pd.read_excel('C:/Users/Raknaros/Desktop/Out_54.xlsx', sheet_name='Sheet1')
#nuevos_customers.drop(['Unnamed: 0','Unnamed: 1','Unnamed: 2'], axis=1, inplace=True)



#nuevos_customers.dropna(axis=0, inplace=True)

#print(nuevos_customers)

print(customers.loc[customers['ruc'] == 10078587909, 'alias'].values[0])