import json
import os
import psycopg2
from lib.create_script import createTableScript
from lib.alter_script import alterTableScript


def insertCommitsCommand(keys, values, json_file):
    sql = "INSERT INTO commits ("
    for i in range(len(keys)):
        atribute_name = keys[i].lower().replace("-", "_")
        if keys[i] == "Commit":
            atribute_name = "commiter"
        elif atribute_name == "user":
            atribute_name = "user_info"
        if i == len(keys) - 1:
            sql += f"{atribute_name}"
        else:
            sql += f"{atribute_name}, "
    sql += ") VALUES (\n"
    for i in range(len(keys)):
        # t = type(json_file[keys[i]])
        if i == len(keys) - 1:
            sql += "%s);"
        else:
            sql += "%s, "

    return sql


def insertIssuesCommand(keys, values, json_file):
    sql = "INSERT INTO issues ("
    for i in range(len(keys)):
        atribute_name = keys[i].lower().replace("-", "_")
        if atribute_name == "user":
            atribute_name = "user_info"
        if i == len(keys) - 1:
            sql += f"{atribute_name}"
        else:
            sql += f"{atribute_name}, "
    sql += ") VALUES (\n"
    for i in range(len(keys)):
        if i == len(keys) - 1:
            sql += "%s);"
        else:
            sql += "%s, "

    return sql


def insertPRsCommand(keys, values, json_file):
    sql = "INSERT INTO pullrequests ("
    for i in range(len(keys)):
        atribute_name = keys[i].lower().replace("-", "_")
        if atribute_name == "user":
            atribute_name = "user_info"
        if i == len(keys) - 1:
            sql += f"{atribute_name}"
        else:
            sql += f"{atribute_name}, "
    sql += ") VALUES (\n"
    for i in range(len(keys)):
        if i == len(keys) - 1:
            sql += "%s);"
        else:
            sql += "%s, "

    return sql


def insertRepositorysCommand(keys, values, json_file):
    sql = "INSERT INTO repositorys ("
    for i in range(len(keys)):
        atribute_name = keys[i].lower().replace("-", "_")
        if atribute_name == "user":
            atribute_name = "user_info"
        if i == len(keys) - 1:
            sql += f"{atribute_name}"
        else:
            sql += f"{atribute_name}, "
    sql += ") VALUES (\n"
    for i in range(len(keys)):
        if i == len(keys) - 1:
            sql += "%s);"
        else:
            sql += "%s, "

    return sql


def _createTable(tables, keys, attributes, list_files, connection, cursor):
    if list_files in tables:
        new_json = {}
        columns = [atrib["name"] for atrib in tables[list_files]]
        for key in attributes:
            name_column = key.lower()
            if name_column == "user":
                name_column = "user_info"
            if not (name_column in columns):
                new_json[key] = attributes[key]
        if len(new_json) > 0:
            keys = [key for key in new_json]
            print(keys)
            alterTableScript(keys, cursor, new_json, list_files)
            connection.commit()
    else:
        createTableScript(keys, cursor, attributes, list_files)
        connection.commit()


def _insert(new_values, keys, values, attributes, cursor, connection, table):
    if table == "commits":
        sql = insertCommitsCommand(keys, values, attributes)
    elif table == "issues":
        sql = insertIssuesCommand(keys, values, attributes)
    elif table == "pullrequests":
        sql = insertPRsCommand(keys, values, attributes)
    elif table == "repositorys":
        sql = insertRepositorysCommand(keys, values, attributes)

    cursor.execute(sql, new_values)
    connection.commit()


def jsonToSql(connection, tables):
    json_commits_files = []
    json_issues_files = []
    json_pullrequests_files = []
    json_repository_files = []
    files = {
        "commits": json_commits_files,
        "issues": json_issues_files,
        "pullrequests": json_pullrequests_files,
        "repositorys": json_repository_files
    }
    for file in os.listdir():
        if file.find('commits.json') != -1:
            json_commits_files.append("./" + file)
        elif file.find('issues.json') != -1:
            json_issues_files.append("./" + file)
        elif file.find('pullrequests.json') != -1:
            json_pullrequests_files.append("./" + file)
        elif file.find('repository.json') != -1:
            json_repository_files.append("./" + file)
    # print(files)
    cursor = connection.cursor()
    for list_files in files:
        attributes = {}
        for file in files[list_files]:
            if os.stat(file).st_size == 0:
                continue
            with open(file) as json_file:
                # print(json_file)
                json_dict = json.load(json_file)
                # attributes = json_dict[0]['data']
                for i in range(len(json_dict)):
                    for key, value in json_dict[i]['data'].items():
                        if not (key in attributes):
                            if not (value is None):
                                attributes[key] = value

                        # if len(json_dict[i]['data']) >= len(attributes) and not (None in json_dict[i]['data'].values()):
                        #     attributes = json_dict[i]['data']

        keys = [key for key in attributes]
        # print(keys)

        _createTable(tables, keys, attributes, list_files, connection, cursor)

    for list_files in files:
        for file in files[list_files]:
            if os.stat(file).st_size == 0:
                continue
            with open(file) as json_file:
                json_dict = json.load(json_file)

                for i in range(len(json_dict)):
                    attributes = json_dict[i]['data']
                    keys = []
                    for key in attributes:
                        if attributes[key] is not None:
                            keys.append(key)

                    values = []
                    for key in attributes:
                        if attributes[key] is not None:
                            values.append(attributes[key])

                    new_values = [json.dumps(v, ensure_ascii=False) if (
                        type(v) is dict or type(v) is list) else v for v in values]

                    _insert(new_values, keys, values, attributes,
                            cursor, connection, list_files)
