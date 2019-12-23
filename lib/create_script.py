def createTableScript(keys, cursor, json_file, table):
    sql = """"""
    sql += f"CREATE TABLE IF NOT EXISTS {table} (\n"
    sql += " key SERIAL"
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
    if table != "repositorys":
        if table == "commits":
            sql += ",\n  PRIMARY KEY (commit)"
        else:
            sql += ",\n  PRIMARY KEY (id)"
    else:
        sql += f",\n PRIMARY KEY ("
        for key in keys:
            if key == keys[-1]:
                sql += f"{key})"
            else:
                sql += f"{key}, "
    sql += "\n);"
    cursor.execute(sql)


def createRelationshipScript(cursor, repository_keys):
    sql = """
    CREATE TABLE IF NOT EXISTS repository_commits_issues_pullrequests (
        key SERIAL,
        commit TEXT,
        id_issue INTEGER,
        id_pull_request INTEGER"""

    for key in repository_keys:
        sql += f",\n\t{key} TEXT"

    sql += ","

    sql += """
        FOREIGN KEY (commit) REFERENCES commits (commit),
        FOREIGN KEY (id_issue) REFERENCES issues (id),
        FOREIGN KEY (id_pull_request) REFERENCES pullrequests (id)"""

    sql += f",\n\tFOREIGN KEY (owner, repository) REFERENCES repositorys (owner, repository)"

    sql += """,
        PRIMARY KEY (commit, id_issue, id_pull_request"""

    for key in repository_keys:
        sql += f", {key}"

    sql += ")\n);"

    print(sql)

    cursor.execute(sql)
