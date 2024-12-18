from flask import Flask, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.orm import sessionmaker

app = Flask(__name__)

# Habilita o CORS para todas as origens
CORS(app, resources={r"/connect/*": {"origins": "http://localhost:8100"}})

# Variáveis globais para a conexão e metadata
engine = None
Session = None
metadata = None

@app.route('/connect/<usuario>/<senha>/<banco>', methods=['GET'])
def config_db(usuario, senha, banco):
    global engine, metadata, Session
    try:
        # Se a conexão não existir ou for diferente, reconectar
        if not engine or engine.url != f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}":
            DATABASE_URL = f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}"
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            Session = sessionmaker(bind=engine)  # Criando a sessão do SQLAlchemy

        # Extrai os nomes das tabelas
        tabelas = list(metadata.tables.keys())

        # Retorna as tabelas e os dados fornecidos
        return jsonify({
            "message": "Dados recebidos com sucesso!",
            "usuario": usuario,
            "banco": banco,
            "tabelas": tabelas
        }), 200
    except Exception as e:
        # Retorna erro caso algo dê errado
        return jsonify({
            "message": "Erro ao conectar ao banco de dados",
            "error": str(e)
        }), 500

@app.route('/connect/columns/<usuario>/<senha>/<banco>/<tabela>', methods=['GET'])
def get_columns(usuario, senha, banco, tabela):
    global engine, metadata, Session
    try:
        # Se a conexão não existir ou for diferente, reconectar
        if not engine or engine.url != f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}":
            DATABASE_URL = f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}"
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            Session = sessionmaker(bind=engine)  # Criando a sessão do SQLAlchemy
        
        # Reflete a tabela específica
        table = Table(tabela, metadata, autoload_with=engine)

        # Extrai os nomes das colunas
        colunas = [column.name for column in table.columns]

        # Retorna as colunas
        return jsonify({
            "message": f"Colunas da tabela {tabela} recebidas com sucesso!",
            "tabela": tabela,
            "colunas": colunas
        }), 200
    except Exception as e:
        # Retorna erro caso algo dê errado
        return jsonify({
            "message": "Erro ao buscar as colunas",
            "error": str(e)
        }), 500
    
@app.route('/connect/data/<usuario>/<senha>/<banco>/<tabela>/<filter>', methods=['GET'])
def get_data_by_column(usuario, senha, banco, tabela, filter):
    global engine, metadata, Session
    try:
        # Se a conexão não existir ou for diferente, reconectar
        if not engine or engine.url != f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}":
            DATABASE_URL = f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}"
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            Session = sessionmaker(bind=engine)  # Criando a sessão do SQLAlchemy
        
        # Reflete a tabela específica
        table = Table(tabela, metadata, autoload_with=engine)

        # Extrai os nomes das colunas
        colunas = [column.name for column in table.columns]

        # Verifica se o filtro (coluna) existe na tabela
        if filter not in colunas:
            return jsonify({
                "message": f"Coluna '{filter}' não encontrada na tabela {tabela}.",
                "colunas": colunas
            }), 400

        # Cria a sessão
        session = Session()

        # Utiliza SQLAlchemy ORM para consultar os dados da coluna com a função text()
        query = text(f"SELECT {filter} FROM {tabela}")
        result = session.execute(query)
        dados_coluna = list(set(row[0] for row in result))

        # Retorna os dados da coluna
        return jsonify({
            "message": f"Dados da coluna '{filter}' da tabela {tabela} recebidos com sucesso!",
            "tabela": tabela,
            "coluna": filter,
            "dados": dados_coluna  # Dados extraídos da coluna
        }), 200

    except Exception as e:
        # Retorna erro caso algo dê errado
        return jsonify({
            "message": "Erro ao buscar os dados da coluna",
            "error": str(e)
        }), 500


@app.route('/connect/value/<usuario>/<senha>/<banco>/<tabela>/<filter>/<value>', methods=['GET'])
def get_filtered_values(usuario, senha, banco, tabela, filter, value):
    global engine, metadata, Session
    try:
        # Se a conexão não existir ou for diferente, reconectar
        if not engine or engine.url != f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}":
            DATABASE_URL = f"postgresql+psycopg2://{usuario}:{senha}@localhost/{banco}"
            engine = create_engine(DATABASE_URL)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            Session = sessionmaker(bind=engine)  # Criando a sessão do SQLAlchemy
        
        # Reflete a tabela específica
        table = Table(tabela, metadata, autoload_with=engine)

        # Extrai os nomes das colunas
        colunas = [column.name for column in table.columns]

        # Verifica se o filtro (coluna) existe na tabela
        if filter not in colunas:
            return jsonify({
                "message": f"Coluna '{filter}' não encontrada na tabela {tabela}.",
                "colunas": colunas
            }), 400

        # Cria a sessão
        session = Session()

        # Consulta os dados filtrados
        query = text(f"SELECT * FROM {tabela} WHERE {filter} = :value")
        result = session.execute(query, {"value": value})
        dados = [tuple(row) for row in result]

        # Identifica colunas válidas (sem valores None ou vazios)
        colunas_validas = [
            i for i, _ in enumerate(colunas)
            if all(row[i] not in (None, "") for row in dados)
        ]

        # Filtra os dados e os nomes das colunas
        colunas_filtradas = [colunas[i] for i in colunas_validas]
        dados_filtrados = [
            tuple(row[i] for i in colunas_validas) for row in dados
        ]

        # Retorna os dados com as colunas filtradas
        return jsonify({
            "message": f"Dados filtrados pela coluna '{filter}' na tabela {tabela} recebidos com sucesso!",
            "tabela": tabela,
            "coluna": filter,
            "colunas_validas": colunas_filtradas,
            "dados": [tuple(colunas_filtradas)] + dados_filtrados  # Inclui os nomes das colunas
        }), 200

    except Exception as e:
        # Retorna erro caso algo dê errado
        return jsonify({
            "message": "Erro ao buscar os dados da tabela",
            "error": str(e)
        }), 500



if __name__ == '__main__':
    app.run(debug=True)
