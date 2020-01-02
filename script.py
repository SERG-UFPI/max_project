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


def createDataBase(new_db, username, password):
    con = psycopg2.connect(dbname='postgres',
                           user=username, host='',
                           password=password)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)  # <-- ADD THIS LINE

    cur = con.cursor()

    # Use the psycopg2.sql module instead of string concatenation
    # in order to avoid sql injection attacs.
    cur.execute(f"CREATE DATABASE {new_db}")


def run(owner, repository, tokens=[]):
    # SENHA DO POSTGRES INSTALADO NO PC É NECESSÁRIA
    # conn = psycopg2.connect(
    #     host="localhost", database="serg", user="postgres", password="")
    conn = psycopg2.connect("postgres://dtatvlptygmaqh:32456b95e7c0d3fef17bf1e41cf373f1c7851807e328141abcc1d2a8402b28f5@ec2-54-163-234-44.compute-1.amazonaws.com:5432/dcsqihk0nk89m9", sslmode='require')

    cursor = conn.cursor()

    repositorys = checkRepoExists(owner, repository, cursor)

    if repositorys is None:
        print("GETING DATA...")

        commits = {}
        issues = {}
        pullrequests = {}

        repository_info = list(generateRepository(owner, repository))
        print("RETRIEVING COMMITS...")
        commits = list(getCommits(owner, repository))
        print("COMMITS RETRIEVED")

        print("RETRIEVING ISSUES...")
        issues = list(getIssues(owner, repository, tokens))
        print("ISSUES RETRIEVED")

        print("RETRIEVING PULL_REQUESTS...")
        pullrequests = list(getPRs(owner, repository, tokens))
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


# if __name__ == '__main__':
#     user_owner = "ES2-UFPI"
#     repo_name = "Unichat"
#     # user_owner = "Mex978"
#     # repo_name = "compilador"
#     run(user_owner, repo_name, tokens=[])
