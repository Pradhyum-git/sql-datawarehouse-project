'''
  We can't do bulk insert in mysql .
  So,we inserted using pandas and sqlalchemy libraries
'''

import pandas as pd
from sqlalchemy import create_engine
try:
  # connecting to the sql database
  username='root'
  password='Pradhyum%402005'
  host='localhost'
  database_name='bronze'
  port=3306
  engine=create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database_name}')
  
  # inserting into crm_cust_info table in bronze database
  
  df=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv',skiprows=1,header=None)
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'crm_cust_info'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df.columns=columns['COLUMN_NAME'].to_list()
  df.to_sql(name='crm_cust_info',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
  
  # inserting into crm_prd_info table in bronze database
  df2=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv',skiprows=1,header=None)
  
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'crm_prd_info'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df2.columns=columns['COLUMN_NAME'].to_list()
  
  df2.to_sql(name='crm_prd_info',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
  
  # inserting sales_details records into crm_sales_details table
  df3=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv',skiprows=1,header=None)
  
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'crm_sales_details'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df3.columns=columns['COLUMN_NAME'].to_list()
  
  df3.to_sql(name='crm_sales_details',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
  
  # inserting CUST_AZ12 rows into erp_cust_az12 table
  df4=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv',skiprows=1,header=None)
  
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'erp_cust_az12'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df4.columns=columns['COLUMN_NAME'].to_list()
  
  df4.to_sql(name='erp_cust_az12',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
  
  # inserting LOC_A101 records into erp_loc_a101 table
  df5=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv',skiprows=1,header=None)
  
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'erp_loc_a101'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df5.columns=columns['COLUMN_NAME'].to_list()
  
  df5.to_sql(name='erp_loc_a101',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
  
  # inserting records into erp_px_cat_giv2 table
  df6=pd.read_csv('/home/rgukt/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv',skiprows=1,header=None)
  
  column_query = """
  SELECT COLUMN_NAME 
  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE TABLE_SCHEMA = 'bronze' 
  AND TABLE_NAME = 'erp_px_cat_g1v2'
  ORDER BY ORDINAL_POSITION
  """
  columns=pd.read_sql(column_query,engine)
  df6.columns=columns['COLUMN_NAME'].to_list()
  
  df6.to_sql(name='erp_px_cat_g1v2',
           con=engine,
           if_exists='replace',
           index=False,
           chunksize=1000)
except Exception as e:
  print('Error message is',e)













