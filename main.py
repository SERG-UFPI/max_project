'''
@Author: Max Nícolas de Oliveira Lima

'''
import psycopg2
import json
import os
from lib.json_to_sql import jsonToSql
from perceval.backends.core.github import GitHub
from perceval.backends.core.git import Git


def getCommits(user_owner, repo_name):
    repo = Git(f"https://github.com/{user_owner}/{repo_name}.git",
               f"https://github.com/{user_owner}/{repo_name}.git")
    commits = repo.fetch()
    return commits


def getIssues(user_owner, repo_name, tokens):
    repo = GitHub(owner=user_owner, repository=repo_name,
                  api_token=tokens, sleep_for_rate=True)
    issues = repo.fetch(category="issue")
    return issues


def getPRs(user_owner, repo_name, tokens):
    repo = GitHub(owner=user_owner, repository=repo_name,
                  api_token=tokens, sleep_for_rate=True)
    prs = repo.fetch(category="pull_request")
    return prs


def getColumnsTable(cursor):
    cursor.execute("""SELECT *
               FROM information_schema.columns
               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
               ORDER BY table_schema, table_name""")
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


def checkRepoExists(user_owner, repo_name, cursor):
    sql = f"""
        SELECT EXISTS (
            SELECT *
            FROM   information_schema.tables
            WHERE  table_schema = 'serg'
            AND    table_name = 'repositorys'
        );
    """

    cursor.execute(sql)
    tables = cursor.fetchall()

    if tables[0][0]:
        sql = f"""
        SELECT
            *
        FROM
            repositorys
        WHERE
            owner = {user_owner} AND
            repository = {repo_name}
        ;"""
        cursor.execute(sql)
        return cursor.fetchall()
    return None


def generateRepository(user_owner, repo_name):
    yield {'data': {
        'owner': user_owner,
        'repository': repo_name
    }}


if __name__ == '__main__':
    # SENHA DO POSTGRES INSTALADO NO PC É NECESSÁRIA
    conn = psycopg2.connect(
        host="localhost", database="serg", user="postgres", password="maxlima13")
    user_owner = "ES2-UFPI"
    repo_name = "Unichat"
    # user_owner = "Mex978"
    # repo_name = "compilador"

    cursor = conn.cursor()

    tokens = [
        "8b2b8e7bc221c6088274c2a30ef35099b1b4e4a5",
        "c1a1231d1f38c478e10dbd06c9d2ac7a05b31817",
        "7933da8e6eafda3d15be7b897f0da0e100a10eae",
        "a2f683c5975ec60f1e59256ed8d59a928427176f",
    ]  # TOKENS DO GITHUB AQUI

    repositorys = checkRepoExists(user_owner, repo_name, cursor)

    if repositorys is None:
        print("GETING DATA...")

        commits = {}
        issues = {}
        pullrequests = {}

        repository_info = list(generateRepository(user_owner, repo_name))
        print("RETRIEVING COMMITS...")
        commits = list(getCommits(user_owner, repo_name))
        print("COMMITS RETRIEVED")

        print("RETRIEVING ISSUES...")
        issues = list(getIssues(user_owner, repo_name, tokens))
        print("ISSUES RETRIEVED")

        print("RETRIEVING PULL_REQUESTS...")
        pullrequests = list(getPRs(user_owner, repo_name, tokens))
        print("PULL_REQUESTS RETRIEVED")

        repository = {
            "repository": repository_info,
            "commits": commits,
            "issues": issues,
            "pullrequests": pullrequests
        }

        print("DATA FETCHED!")

        tables = getColumnsTable(cursor)
        jsonToSql(conn, tables, repository)
    conn.close()
