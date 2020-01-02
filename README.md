# Ferramenta para coleta e persistência de dados de repositórios de software

## Requisitos
* Ter o python na em versão >= 3.6
* Instale os módulos que estão no arquivo requirements.txt com o comando abaixo:

```sh
pip install -r requirements.txt
```

## API
A ferramenta em questão pode ser utilizada publicamente através do url serg-ufpi.herokuapp.com

### Headers
Para utilização da API é necessário que nos headers sejam passadas as seguintes informações
* `Content-Type: application/json`
* tokens: Uma lista de tokens para que a ferramenta não tenha obstáculos com relação a limite de requisições ao github

### A api possui os seguintes endpoints no momento
* /insert
  * Tipo do método: POST
  * Informações no body (json):
    * owner: Nome do dono do repositório
    * repository: Nome do repositório
    


