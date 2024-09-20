import psycopg2
import re
from datetime import datetime

def conectar_banco():
    try:
        conn = psycopg2.connect(
            dbname="banco",
            user="usuario",
            password="senha",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def criar_esquema(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS produtos (
        id SERIAL PRIMARY KEY,
        asin VARCHAR(10) UNIQUE,
        titulo VARCHAR(255),
        grupo VARCHAR(50),
        salesrank INT
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS reviews (
        id SERIAL PRIMARY KEY,
        produto_id INT REFERENCES produtos(id),
        data DATE,
        cliente_id VARCHAR(50),
        classificacao INT,
        votos INT,
        util INT
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS categorias (
        id SERIAL PRIMARY KEY,
        nome VARCHAR(255) UNIQUE
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS produto_categoria (
        produto_id INT REFERENCES produtos(id),
        categoria_id INT REFERENCES categorias(id)
    );''')

def inserir_dados_produto(cursor, asin, titulo, grupo, salesrank):
    cursor.execute('''INSERT INTO produtos (asin, titulo, grupo, salesrank)
                      VALUES (%s, %s, %s, %s)
                      ON CONFLICT (asin) DO NOTHING''', (asin, titulo, grupo, salesrank))

def inserir_dados_review(cursor, produto_id, data, cliente_id, classificacao, votos, util):
    cursor.execute('''INSERT INTO reviews (produto_id, data, cliente_id, classificacao, votos, util)
                      VALUES (%s, %s, %s, %s, %s, %s)''', (produto_id, data, cliente_id, classificacao, votos, util))

def processar_arquivo(cursor, arquivo):
    produto_atual = {}
    
    with open(arquivo, 'r') as file:
        for linha in file:
            linha = linha.strip()
            
            if linha.startswith("Id:"):
                produto_atual = {}
            elif linha.startswith("ASIN:"):
                produto_atual['asin'] = linha.split(":")[1].strip()
            elif linha.startswith("title:"):
                produto_atual['title'] = linha.split(":")[1].strip()
            elif linha.startswith("group:"):
                produto_atual['group'] = linha.split(":")[1].strip()
            elif linha.startswith("salesrank:"):
                produto_atual['salesrank'] = int(linha.split(":")[1].strip())
            elif linha.startswith("reviews:"):
                pass
            elif re.match(r"\d{4}-\d{1,2}-\d{1,2}", linha):
                partes = linha.split()
                data = datetime.strptime(partes[0], "%Y-%m-%d").date()
                cliente_id = partes[2]
                classificacao = int(partes[4])
                votos = int(partes[6])
                util = int(partes[8])
                
                inserir_dados_produto(cursor, produto_atual['asin'], produto_atual.get('title'), produto_atual.get('group'), produto_atual.get('salesrank'))
                cursor.execute('SELECT id FROM produtos WHERE asin=%s', (produto_atual['asin'],))
                produto_id = cursor.fetchone()[0]
                inserir_dados_review(cursor, produto_id, data, cliente_id, classificacao, votos, util)

            elif linha.startswith("categories:"):
                categorias = linha.split("|")[1:]  
                for categoria in categorias:
                    nome_categoria = categoria.strip().split("[")[0] 
                    cursor.execute('''INSERT INTO categorias (nome) VALUES (%s)
                                      ON CONFLICT (nome) DO NOTHING''', (nome_categoria,))
                    cursor.execute('SELECT id FROM categorias WHERE nome=%s', (nome_categoria,))
                    categoria_id = cursor.fetchone()[0]
                    
                    cursor.execute('''INSERT INTO produto_categoria (produto_id, categoria_id)
                                      VALUES (%s, %s)''', (produto_id, categoria_id))

def main():
    conn = conectar_banco()
    if conn is None:
        return

    cursor = conn.cursor()
    
    try:
        criar_esquema(cursor)
        processar_arquivo(cursor, 'amazon-meta.txt')
        conn.commit()
    except Exception as e:
        print(f"Erro ao processar o arquivo ou inserir dados: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
