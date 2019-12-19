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


if __name__ == '__main__':

    # SENHA DO POSTGRES INSTALADO NO PC É NECESSÁRIA
    conn = psycopg2.connect(
        host="localhost", database="serg", user="postgres", password="")
    user_owner = "ES2-UFPI"
    repo_name = "Unichat"
    # user_owner = "Mex978"
    # repo_name = "compilador"
    print("GETING DATA...")

    # condition = (os.path.exists(f"./{user_owner}_{repo_name}_commits.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_issues.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_pullrequests.json") and
    #              os.path.exists(f"./{user_owner}_{repo_name}_repository.json"))

    token = None  # TOKEN DO GITHUB AQUI

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
    jsonToSql(conn)
    conn.close()
