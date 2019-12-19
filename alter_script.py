def alterTableScript(keys, cursor, json_file, table):
    sql = """"""
    sql += f"ALTER TABLE IF EXISTS {table}\n"
    for key in keys:
        t = type(json_file[key])
        atribute_name = key.lower()
        sql += "ADD COLUMN"
        if keys[-1] == key:
            if t is bool:
                sql += f" {atribute_name} BOOLEAN;"
            elif t is str:
                sql += f" {atribute_name} TEXT;"
            elif t is list or t is dict:
                sql += f" {atribute_name} JSON;"
            elif t is int:
                sql += f" {atribute_name} INTEGER;"
            elif t is float:
                sql += f" {atribute_name} DECIMAL;"
        else:
            if t is bool:
                sql += f" {atribute_name} BOOLEAN,\n"
            elif t is str:
                sql += f" {atribute_name} TEXT,\n"
            elif t is list or t is dict:
                sql += f" {atribute_name} JSON,\n"
            elif t is int:
                sql += f" {atribute_name} INTEGER,\n"
            elif t is float:
                sql += f" {atribute_name} DECIMAL,\n"
    # print(sql)
    cursor.execute(sql)
