import os


# def createCommitsScript(keys, cursor, json_file):
#     sql = """"""
#     sql += "CREATE TABLE IF NOT EXISTS commits (\n"
#     sql += "  key SERIAL PRIMARY KEY,\n"
#     for key in keys:
#         t = type(json_file[key])
#         atribute_name = key.lower()
#         if key == "Commit":
#             atribute_name = "commiter"
#         elif atribute_name == "user":
#             atribute_name = "user_info"
#         if keys[-1] == key:
#             if t is bool:
#                 sql += f"  {atribute_name} BOOLEAN\n"
#             elif t is str:
#                 sql += f"  {atribute_name} TEXT\n"
#             elif t is list or t is dict:
#                 sql += f"  {atribute_name} JSON\n"
#             elif t is int:
#                 sql += f"  {atribute_name} INTEGER\n"
#         else:
#             if t is bool:
#                 sql += f"  {atribute_name} BOOLEAN,\n"
#             elif t is str:
#                 sql += f"  {atribute_name} TEXT,\n"
#             elif t is list or t is dict:
#                 sql += f"  {atribute_name} JSON,\n"
#             elif t is int:
#                 sql += f"  {atribute_name} INTEGER,\n"
#     sql += ");"
#     cursor.execute(sql)


# def createIssuesScript(keys, cursor, json_file):
#     sql = """"""
#     sql += "CREATE TABLE IF NOT EXISTS issues (\n"
#     sql += " key SERIAL PRIMARY KEY,\n"
#     for key in keys:
#         t = type(json_file[key])
#         # print(t)
#         atribute_name = key.lower()
#         if atribute_name == "user":
#             atribute_name = "user_info"
#         if keys[-1] == key:
#             if t is bool:
#                 sql += f" {atribute_name} BOOLEAN\n"
#             elif t is str:
#                 sql += f" {atribute_name} TEXT\n"
#             elif t is list or t is dict:
#                 sql += f" {atribute_name} JSON\n"
#             elif t is int:
#                 sql += f" {atribute_name} INTEGER\n"
#         else:
#             if t is bool:
#                 sql += f" {atribute_name} BOOLEAN,\n"
#             elif t is str:
#                 sql += f" {atribute_name} TEXT,\n"
#             elif t is list or t is dict:
#                 sql += f" {atribute_name} JSON,\n"
#             elif t is int:
#                 sql += f" {atribute_name} INTEGER,\n"
#     sql += ");"
#     # print(sql)
#     cursor.execute(sql)


def createTableScript(keys, cursor, json_file, table):
    sql = """"""
    sql += f"CREATE TABLE IF NOT EXISTS {table} (\n"
    sql += " key SERIAL PRIMARY KEY,\n"
    for key in keys:
        t = type(json_file[key])
        # print(t)
        atribute_name = key.lower().replace("-", "_")
        if key == "Commit":
            atribute_name = "commiter"
        if atribute_name == "user":
            atribute_name = "user_info"
        if keys[-1] == key:
            if t is bool:
                sql += f" {atribute_name} BOOLEAN\n"
            elif t is str:
                sql += f" {atribute_name} TEXT\n"
            elif t is list or t is dict:
                sql += f" {atribute_name} JSON\n"
            elif t is int:
                sql += f" {atribute_name} INTEGER\n"
            elif t is float:
                sql += f" {atribute_name} DECIMAL\n"
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
    sql += ");"
    # print(sql)
    cursor.execute(sql)


# def createRepositorysScript(keys, cursor, json_file):
#     sql = """"""
#     sql += "CREATE TABLE IF NOT EXISTS repositorys (\n"
#     sql += " key SERIAL PRIMARY KEY,\n"
#     for key in keys:
#         t = type(json_file[key])
#         # print(t)
#         atribute_name = key.lower()
#         if atribute_name == "user":
#             atribute_name = "user_info"
#         if keys[-1] == key:
#             if t is bool:
#                 sql += f" {atribute_name} BOOLEAN\n"
#             elif t is str:
#                 sql += f" {atribute_name} TEXT\n"
#             elif t is list or t is dict:
#                 sql += f" {atribute_name} JSON\n"
#             elif t is int:
#                 sql += f" {atribute_name} INTEGER\n"
#         else:
#             if t is bool:
#                 sql += f" {atribute_name} BOOLEAN,\n"
#             elif t is str:
#                 sql += f" {atribute_name} TEXT,\n"
#             elif t is list or t is dict:
#                 sql += f" {atribute_name} JSON,\n"
#             elif t is int:
#                 sql += f" {atribute_name} INTEGER,\n"
#     sql += ");"
#     # print(sql)
#     cursor.execute(sql)
