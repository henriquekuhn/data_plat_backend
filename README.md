# Índice

1. [Configuração do Ambiente](#1-configuração-do-ambiente)
   - 1.1. [Instalação de Ferramentas Necessárias](#11-instalação-de-ferramentas-necessárias)
     - [Python](#python)
     - [Docker](#docker)
     - [PostgreSQL (via Docker)](#postgresql-via-docker)
     - [Node.js e Ionic](#nodejs-e-ionic)
   - 1.2. [Configuração do Docker e PostgreSQL](#12-configuração-do-docker-e-postgresql)
     - [Criação do Arquivo `docker-compose.yml`](#criação-do-arquivo-docker-composeyml)
   - 1.3. [Configuração do Banco de Dados](#13-configuração-do-banco-de-dados)
     - [Acessando o PostgreSQL](#acessando-o-postgresql)
   - 1.4. [Backup e Restauração do Contêiner](#14-backup-e-restauração-do-contêiner)
     - [Gerando um Backup](#gerando-um-backup)
     - [Restaurando o Backup](#restaurando-o-backup)

2. [Documentação do Backend](#2-documentação-do-backend)
   - 2.1. [Tecnologias Utilizadas](#21-tecnologias-utilizadas)
   - 2.2. [Variáveis Globais](#22-variáveis-globais)
   - 2.3. [Rotas e Funcionalidades](#23-rotas-e-funcionalidades)
   - 2.4. [Exemplo de Uso](#24-exemplo-de-uso)

3. [Documentação do Frontend](#3-documentação-do-frontend)
   - 3.1. [Tecnologias Utilizadas](#31-tecnologias-utilizadas)
   - 3.2. [Variáveis de Estado](#32-variáveis-de-estado)
   - 3.3. [Funcionalidades Principais](#33-funcionalidades-principais)
   - 3.4. [Interface do Usuário](#34-interface-do-usuário)

---

# Documentação do Projeto: Banco de Dados e Interface de Busca

Este documento descreve o passo a passo para configurar o ambiente, gerenciar o banco de dados PostgreSQL com Docker, e utilizar o componente de busca desenvolvido em **React com Ionic** e **TypeScript**. O objetivo é fornecer uma visão clara e detalhada para que outros programadores possam dar continuidade ao projeto.

---

## 1. Configuração do Ambiente

### 1.1. Instalação de Ferramentas Necessárias

#### **Python**
1. **Baixe e Instale o Python:**
   - Acesse [python.org](https://www.python.org/downloads/) e baixe a versão mais recente para Windows.
   - Durante a instalação, marque a opção **"Add Python to PATH"**.
   
2. **Verifique a Instalação:**
   - Abra o Prompt de Comando ou PowerShell e execute:
     ```bash
     python --version
     ```
   - Isso deve exibir a versão do Python instalada.

#### **Docker**
1. **Baixe e Instale o Docker:**
   - Acesse [Docker Desktop](https://www.docker.com/products/docker-desktop) e baixe a versão para Windows.
   - Siga as instruções de instalação.

2. **Verifique a Instalação:**
   - Abra o Prompt de Comando ou PowerShell e execute:
     ```bash
     docker --version
     ```
   - Isso deve exibir a versão do Docker instalada.

#### **PostgreSQL (via Docker)**
O PostgreSQL será executado em um contêiner Docker. A instalação é feita automaticamente ao configurar o `docker-compose.yml`.

#### **Node.js e Ionic**
1. **Baixe e Instale o Node.js:**
   - Acesse [nodejs.org](https://nodejs.org/) e baixe a versão LTS para Windows.
   - Siga as instruções de instalação.

2. **Instale o Ionic CLI:**
   - No Prompt de Comando ou PowerShell, execute:
     ```bash
     npm install -g @ionic/cli
     ```
   - Verifique a instalação:
     ```bash
     ionic --version
     ```

---

### 1.2. Configuração do Docker e PostgreSQL

#### **Criação do Arquivo `docker-compose.yml`**
1. **Crie um diretório para o projeto:**

   ```bash
   mkdir data-plat
   cd data-plat
   ```
2. **Crie o arquivo docker-compose.yml:**

-   Abra um editor de texto e adicione o seguinte conteúdo:

```yaml
version: '3.8'
services:
  db:
    image: postgres:latest
    environment:
      POSTGRES_USER: cafrunikuhn
      POSTGRES_PASSWORD: admin
      POSTGRES_DB: data_plat
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

3. **Inicie o contêiner:**

```bash
docker-compose up -d
```

4. **Verifique se o contêiner está em execução:**

```bash
docker ps
```
-   O contêiner data-plat-db-1 deve estar listado.

### 1.3. Configuração do Banco de Dados
#### **Acessando o PostgreSQL**

1. **Acesse o contêiner PostgreSQL:**

```bash
docker exec -it data-plat-db-1 psql -U cafrunikuhn -d data_plat
```

2. **Crie uma tabela de teste:**

```sql
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);
```

3. **Insira dados na tabela:**

```sql
INSERT INTO test_table (name) VALUES ('Test Data 1'), ('Test Data 2');
```

4. **Verifique os dados inseridos:**

```sql
SELECT * FROM test_table;
```

### 1.4. Backup e Restauração do Contêiner
#### **Gerando um Backup**

1. **Identifique o ID do contêiner:**

```bash
docker ps -a
```

2. **Exporte o contêiner:**

```bash
docker export <CONTAINER_ID> > container_backup.tar
```

### **Restaurando o Backup**

1. **Importe o backup:**

```bash
docker import container_backup.tar new_container_image
```

2. **Crie um novo contêiner:**

```bash
docker run -d --name restored_container new_container_image
```

## 2. Documentação do Backend

### 2.1. Tecnologias Utilizadas
-   Linguagem: Python
-   Framework: Flask
-   Bibliotecas: Flask-CORS, SQLAlchemy

### 2.2. Variáveis Globais

| Variável       | Descrição                                                                 |
|----------------|---------------------------------------------------------------------------|
| engine       | Armazena a conexão com o banco de dados.                                  |
| Session      | Armazena a sessão do SQLAlchemy para executar consultas no banco de dados.|
| metadata     | Armazena os metadados do banco de dados, como tabelas e colunas.          |

---

### 2.3. Rotas e Funcionalidades

| Rota                                      | Descrição                                                                 |
|-------------------------------------------|---------------------------------------------------------------------------|
| `GET /connect/<usuario>/<senha>/<banco>`  | Conecta ao banco de dados e lista as tabelas.                             |
| `GET /connect/columns/<usuario>/<senha>/<banco>/<tabela>` | Lista as colunas de uma tabela.                          |
| `GET /connect/data/<usuario>/<senha>/<banco>/<tabela>/<filter>` | Retorna valores únicos de uma coluna.           |
| `GET /connect/value/<usuario>/<senha>/<banco>/<tabela>/<filter>/<value>` | Filtra dados por valor específico. |

### 2.4. Exemplo de Uso

1. **Conectar ao Banco de Dados**

Requisição:

```
bash
GET /connect/admin/senha123/meu_banco
```

Resposta:

```
json
{
  "message": "Dados recebidos com sucesso!",
  "usuario": "admin",
  "banco": "meu_banco",
  "tabelas": ["tabela1", "tabela2"]
}
```

2. **Listar Colunas de uma Tabela**

Requisição:

```
bash
GET /connect/columns/admin/senha123/meu_banco/tabela1
```

Resposta:

```
json
{
  "message": "Colunas da tabela tabela1 recebidas com sucesso!",
  "tabela": "tabela1",
  "colunas": ["coluna1", "coluna2"]
}
```

3. **Filtrar Dados por Coluna**

Requisição:

```
bash
GET /connect/data/admin/senha123/meu_banco/tabela1/coluna1
```

Resposta:

```
json
{
  "message": "Dados da coluna 'coluna1' da tabela tabela1 recebidos com sucesso!",
  "tabela": "tabela1",
  "coluna": "coluna1",
  "dados": ["valor1", "valor2"]
}
```

4. **Filtrar Dados por Valor Específico**

Requisição:

```
bash
GET /connect/value/admin/senha123/meu_banco/tabela1/coluna1/valor1
```

Resposta:

```
json
{
  "message": "Dados filtrados pela coluna 'coluna1' na tabela tabela1 recebidos com sucesso!",
  "tabela": "tabela1",
  "coluna": "coluna1",
  "colunas_validas": ["coluna1", "coluna2"],
  "dados": [
    ["coluna1", "coluna2"],
    ["valor1", "dado1"],
    ["valor1", "dado2"]
  ]
}
```

## 3. Documentação do Frontend
### 3.1. Tecnologias Utilizadas

-   Linguagem: TypeScript (TSX)
-   Framework: React com Ionic
-   Bibliotecas: ReactTabulator, Ionic Components

### 3.2. Variáveis de Estado

| Variável               | Descrição                                                                 |
|------------------------|---------------------------------------------------------------------------|
| selectedDatabase     | Armazena o banco de dados selecionado pelo usuário.                       |
| filterOptions        | Lista de tabelas disponíveis no banco de dados selecionado.               |
| selectedTable        | Armazena a tabela escolhida pelo usuário para consulta.                   |
| tableColumns         | Lista de colunas disponíveis na tabela selecionada.                       |
| selectedValue        | Armazena os valores disponíveis para filtragem na coluna selecionada.     |
| selectedColumn       | Armazena a coluna escolhida para aplicar filtros.                         |
| selectedColumnData   | Armazena o valor específico selecionado para filtrar os dados.            |
| handleData           | Armazena os dados retornados da consulta para exibição na tabela.         |
| numOfPass, numOfFail | Contam quantos resultados passaram ou falharam em um teste.              |
| totalNum             | Armazena o número total de resultados retornados na consulta.             |
| highestResult, lowestResult, avgResult | Armazenam o maior, menor e a média dos valores numéricos.         |
| errorAlert, toastMessage, showToast | Gerenciam mensagens de erro e notificações exibidas ao usuário. |
| loading              | Controla o estado de carregamento, exibindo uma mensagem durante operações. |

### 3.3. Funcionalidades Principais

-   handleConnectClick: Conecta-se ao banco de dados selecionado e carrega as tabelas disponíveis.
-   handleSelectTable: Busca as colunas da tabela selecionada.
-   handleSelectValue: Obtém os valores disponíveis na coluna selecionada.
-   getDataTable: Realiza uma consulta com base nas seleções do usuário e exibe os dados na tabela.

### 3.4. Interface do Usuário
O componente renderiza uma interface intuitiva com:

-   Seletores: Para escolher banco de dados, tabelas, colunas e valores.
-   Botões: "Connect" para conectar ao banco de dados e "Get Data" para buscar os dados.
-   Tabela Interativa: Exibe os dados retornados usando ReactTabulator.
-   Estatísticas: Mostra o total de dados, passos/falhas, maior/menor valor e média.
-   Notificações: Mensagens de erro ou sucesso são exibidas usando IonToast.
