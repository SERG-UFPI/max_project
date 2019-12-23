import json
import os
import psycopg2
from lib.create_script import createTableScript, createRelationshipScript
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


def insertRepositorysRelationshipCommand():
    sql = f"""
        INSERT INTO 
            repository_commits_issues_pullrequests (owner, repository)
        VALUES
            (%s, %s);
    """
    return sql


def updateRepositorysRelationshipCommand(cursor, value, table_referenced, repository_dict):
    if table_referenced == "issues":
        atributte_to_update = "id_issue"
    elif table_referenced == "pullrequests":
        atributte_to_update = "id_pull_request"
    elif table_referenced == "commits":
        atributte_to_update = "commit"
    sql = f"""
    UPDATE repository_commits_issues_pullrequests set {atributte_to_update} = %s
    WHERE """

    for key in repository_dict:
        if key == list(repository_dict.keys())[-1]:
            sql += f"{key} = {repository_dict[key]});"
        else:
            sql += f"{key} = {repository_dict[key]} AND "

    cursor.execute(sql, value)


def _createTable(tables, keys, attributes, category, connection, cursor):
    table = 'repositorys' if category == 'repository' else category
    if table in tables:
        new_json = {}
        columns = [atrib["name"] for atrib in tables[table]]
        for key in attributes:
            name_column = key.lower()
            if name_column == "user":
                name_column = "user_info"
            if not (name_column in columns):
                new_json[key] = attributes[key]
        if len(new_json) > 0:
            keys = [key for key in new_json]
            alterTableScript(keys, cursor, new_json, table)
            connection.commit()
    else:
        createTableScript(keys, cursor, attributes, table)
        connection.commit()


def _createRelationshipTable(connection, cursor, keys):
    createRelationshipScript(cursor, keys)
    connection.commit()


def _insert(new_values, keys, values, attributes, cursor, connection, category):
    table = 'repositorys' if category == 'repository' else category
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


def jsonToSql(connection, tables, repository):
    print("PARSING TO SQL...")
    cursor = connection.cursor()

    # Criação da tabela
    for category in repository:
        attributes = {}
        for item in repository[category]:
            for key, value in item['data'].items():
                if not (value is None):
                    attributes[key] = value

        keys = [key for key in attributes]
        print(f"CREATING TABLE {category}")
        _createTable(tables, keys, attributes, category, connection, cursor)

    for item in repository["repository"]:
        attributes = {}
        for key, value in item['data'].items():
            if not (value is None):
                attributes[key] = value

        keys = [key for key in attributes]
        _createRelationshipTable(connection, cursor, keys)

    # Inserção de items do db
    for category in repository:
        attributes = {}
        for item in repository[category]:
            attributes = item['data']
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
            print(f"INSERTING DATA IN DB {category}")
            _insert(new_values, keys, values, attributes,
                    cursor, connection, category)
            if category == "repository":
                sql = insertRepositorysRelationshipCommand()
                cursor.execute(
                    sql, (attributes["owner"], attributes["repository"]))
                connection.commit()
            else:
                if category == "commits":
                    updateRepositorysRelationshipCommand(
                        cursor, attributes["commit"], category, repository["repository"][0]["data"])
                else:
                    print("====================================================")
                    print(attributes)
                    print("====================================================")
                    updateRepositorysRelationshipCommand(
                        cursor, attributes["id"], category, repository["repository"][0]["data"])
