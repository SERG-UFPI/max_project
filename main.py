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
        host="localhost", database="serg", user="postgres", password="")
    user_owner = "ES2-UFPI"
    repo_name = "Unichat"
    # user_owner = "Mex978"
    # repo_name = "compilador"

    cursor = conn.cursor()

    tokens = [

    ]  # TOKENS DO GITHUB AQUI

    repositorys = checkRepoExists(user_owner, repo_name, cursor)

    if repositorys is None:
        print("GETING DATA...")

        commits = {}
        issues = {}
        pullrequests = {}

        repository_info = list(generateRepository(user_owner, repo_name))
        commits = list(getCommits(user_owner, repo_name))
        issues = list(getIssues(user_owner, repo_name, tokens))
        print("ISSUES RETRIEVED")
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
