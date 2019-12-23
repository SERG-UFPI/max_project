import json
import os
import psycopg2
from lib.create_script import createTableScript, createRelationshipCommitsRepositorysScript, createRelationshipIssuesRepositorysScript, createRelationshipPullRequestsRepositorysScript
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


# def insertRepositorysRelationshipCommand():
#     sql = f"""
#         INSERT INTO
#             repository_commits_issues_pullrequests (owner, repository, commit, id_issue, id_pull_request,)
#         VALUES
#             (%s, %s);
#     """
#     return sql


def insertRepositorysRelationshipCommand(cursor, values, table_referenced):
    relationship_table = ""
    atributte = ""
    if table_referenced == "issues":
        relationship_table = "repository_issues"
        atributte = "id_issue"
    elif table_referenced == "pullrequests":
        relationship_table = "repository_pullrequests"
        atributte = "id_pull_request"
    elif table_referenced == "commits":
        relationship_table = "repository_commits"
        atributte = "commit"
    sql = f"""
    INSERT INTO {relationship_table} (owner, repository, {atributte})
    VALUES (%s, %s, %s);"""

    cursor.execute(sql, values)


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
    createRelationshipCommitsRepositorysScript(cursor, keys)
    createRelationshipIssuesRepositorysScript(cursor, keys)
    createRelationshipPullRequestsRepositorysScript(cursor, keys)
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

    owner_name = repository["repository"][0]["data"]["owner"]
    repository_name = repository["repository"][0]["data"]["repository"]

    # Criação da tabela
    for category in repository:
        attributes = {}
        for item in repository[category]:
            for key, value in item['data'].items():
                if not (value is None):
                    attributes[key] = value

        keys = [key for key in attributes]

        try:
            _createTable(tables, keys, attributes,
                         category, connection, cursor)
            print(f"CREATED TABLE {category}")
        except Exception as e:
            print(f" # Erro na criação da tabela: {e}")

    for item in repository["repository"]:
        attributes = {}
        for key, value in item['data'].items():
            if not (value is None):
                attributes[key] = value

        keys = [key for key in attributes]

        try:
            _createRelationshipTable(connection, cursor, keys)
            print(f"CREATED RELATIONSHIP TABLES")
        except Exception as e:
            print(f" # Erro na criação de tabelas de relacionamento: {e}")

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

            try:
                _insert(new_values, keys, values, attributes,
                        cursor, connection, category)
                if category != "repository":
                    if category == "commits":
                        insertRepositorysRelationshipCommand(
                            cursor, (owner_name, repository_name, attributes["commit"]), category)
                    else:
                        insertRepositorysRelationshipCommand(
                            cursor, (owner_name, repository_name, attributes["id"]), category)

                print(f"INSERTED DATA IN DB {category}")
            except Exception as e:
                print(f" # Erro na inserção de dados: {e}")
