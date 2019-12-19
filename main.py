'''
@Author: Max Nícolas de Oliveira Lima

'''
import os
import json
import psycopg2
from json_to_sql import jsonToSql


def getCommits(user_owner, repo_name):
    command = f"perceval git \"https://github.com/{user_owner}/{repo_name}.git\" -o {user_owner}_{repo_name}_commits.json"
    print(command)
    os.system(command)


def getIssues(user_owner, repo_name, token):
    command = f"perceval github {user_owner} {repo_name} --category issue -t {token} --sleep-for-rate -o {user_owner}_{repo_name}_issues.json"
    # print(command)
    os.system(command)


def getPRs(user_owner, repo_name, token):
    command = f"perceval github {user_owner} {repo_name} --category pull_request -t {token} --sleep-for-rate -o {user_owner}_{repo_name}_pullrequests.json"
    # print(command)
    os.system(command)


def getRepository(user_owner, repo_name, token):
    command = f"perceval github {user_owner} {repo_name} --category repository -t {token} --sleep-for-rate -o {user_owner}_{repo_name}_repository.json"
    # print(command)
    os.system(command)


def fixJson(user_owner, repo_name):
    files = [
        f"./{user_owner}_{repo_name}_commits.json",
        f"./{user_owner}_{repo_name}_issues.json",
        f"./{user_owner}_{repo_name}_pullrequests.json",
        f"./{user_owner}_{repo_name}_repository.json"
    ]
    for f in files:
        if os.stat(f).st_size == 0:
            continue
        file = open(f, "r")
        linha = file.readline()
        if (linha.find("[{") != -1):
            continue
        linha = linha.replace("{", "[{")
        resto = file.readlines()
        resto[len(resto) - 1] = resto[len(resto) - 1].replace("}", "}]")
        file.close()
        file = open(f, "w")
        file.write(linha)
        for line in resto:
            file.write(line)
        file.close()
        file = open(f, "r")
        content = file.read()
        content = content.replace("}\n{", "},\n{")
        file.close()
        file = open(f, "w")
        file.write(content)
        file.close()


def getColumnsTable(cursor, table):
    # cursor.execut(f"""
    #     SELECT * FROM {table} LIMIT 0;
    # """)

    cursor.execute("""select *
               from information_schema.columns
               where table_schema NOT IN ('information_schema', 'pg_catalog')
               order by table_schema, table_name""")
    tables = {}
    for row in cursor:
        table = row[2]
        column = row[3]
        type_column = row[7]

        if table in tables:
            tables[table].append({
                "name": row[3],
                "type": row[7]
            })
        else:
            tables[table] = [{
                "name": row[3],
                "type": row[7]
            }]

    return tables


if __name__ == '__main__':

    # SENHA DO POSTGRES INSTALADO NO PC É NECESSÁRIA
    conn = psycopg2.connect(
        host="localhost", database="serg", user="postgres", password="maxlima13")
    user_owner = "ES2-UFPI"
    repo_name = "Unichat"
    # user_owner = "Mex978"
    # repo_name = "compilador"
    print("GETING DATA...")

    # condition = (os.path.exists(f"./{user_owner}_{repo_name}_commits.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_issues.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_pullrequests.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_repository.json"))

    token = "9da46c40b0335b8c5d08fa7304b84f3950c9ff45"  # TOKEN DO GITHUB AQUI

    if not os.path.exists(f"./{user_owner}_{repo_name}_commits.json"):
        getCommits(user_owner, repo_name)
    if not os.path.exists(f"./{user_owner}_{repo_name}_issues.json"):
        getIssues(user_owner, repo_name, token)
    if not os.path.exists(f"./{user_owner}_{repo_name}_pullrequests.json"):
        getPRs(user_owner, repo_name, token)
    if not os.path.exists(f"./{user_owner}_{repo_name}_repository.json"):
        getRepository(user_owner, repo_name, token)

    fixJson(user_owner, repo_name)
    print("DATA FETCHED!")
    # c = conn.cursor()
    tables = getColumnsTable(conn.cursor(), None)
    jsonToSql(conn, tables)
    conn.close()
