from migration import Migration

load_tables=['NATION', 'REGION', 'PART', 'SUPPLIER', 'PARTSUPP', 'CUSTOMER', 'ORDERS', 'LINEITEM']
conn_string_in="host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
conn_string_out="host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"

db_migration = Migration(conn_string_in, conn_string_out)
print('------download_data_by_tables------')
db_migration.download_data_by_tables(load_tables)
print('------load_data_by_tables------')
db_migration.load_data_by_tables(load_tables)
print('------check_loaded_data_by_tables------')
db_migration.check_loaded_data_by_tables(load_tables)
