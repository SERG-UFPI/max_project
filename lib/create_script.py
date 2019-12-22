def createTableScript(keys, cursor, json_file, table):
    sql = """"""
    sql += f"CREATE TABLE IF NOT EXISTS {table} (\n"
    sql += " key SERIAL PRIMARY KEY"
    for key in keys:
        t = type(json_file[key])
        atribute_name = key.lower().replace("-", "_")
        if key == "Commit":
            atribute_name = "commiter"
        if atribute_name == "user":
            atribute_name = "user_info"
        if t is bool:
            sql += f",\n {atribute_name} BOOLEAN"
        elif t is str:
            sql += f",\n {atribute_name} TEXT"
        elif t is list or t is dict:
            sql += f",\n {atribute_name} JSON"
        elif t is int:
            sql += f",\n {atribute_name} INTEGER"
        elif t is float:
            sql += f",\n {atribute_name} DECIMAL"
    sql += ",\n  PRIMARY KEY (key, id)"
    sql += "\n);"
    cursor.execute(sql)
