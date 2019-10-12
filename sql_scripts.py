# sql scripts
sql_table_exist = "SELECT count(*) > 0 FROM sqlite_master WHERE type='table' and name='{table_name}'"
sql_row_count = "SELECT count(*) FROM {table_name}"
sql_insert_values = "INSERT INTO {table_name} ({col_names}) VALUES ({csv_row})"
sql_select_certain_columns = "SELECT {column_names} FROM {table_name} WHERE {col_name} = '{col_value}'"
sql_select_all = "SELECT {column_names} FROM {table_name}"
sql_drop_table = "DROP TABLE {table_name}"
sql_truncate_table = "DELETE FROM {table_name}"
sql_create_table = "CREATE TABLE {table_name} (id integer primary key autoincrement, {columns})"
sql_create_table_1fk = """CREATE TABLE {table_name}
 (id integer primary key autoincrement, {columns},
  FOREIGN KEY ({key_name})
  REFERENCES {other_table} ({other_pk}))"""
sql_create_table_2fk = """CREATE TABLE {table_name}
 (id integer primary key autoincrement, {columns},
  FOREIGN KEY ({key_name1})
  REFERENCES {other_table1} ({other_pk1}),
  FOREIGN KEY ({key_name2})
  REFERENCES {other_table2} ({other_pk2}))"""
sql_group_by = 'SELECT count(*), {col_name} FROM {table_name} group by {col_name}'
sql_group_by_limit = """SELECT count(*) as {placeholder}, {col_name}
 FROM {table_name}
 GROUP BY {col_name}
 ORDER BY {order_by} {direction}
 LIMIT {num_lines};"""