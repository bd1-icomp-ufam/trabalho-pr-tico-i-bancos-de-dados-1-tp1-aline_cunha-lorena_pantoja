import psycopg2

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

def listar_top_reviews(produto_asin):
    conn = conectar_banco()
    if conn is None:
        return
    cursor = conn.cursor()

    try:
        cursor.execute('''
        SELECT cliente_id, classificacao, votos, util, data
        FROM reviews
        JOIN produtos ON produtos.id = reviews.produto_id
        WHERE produtos.asin = %s
        ORDER BY util DESC, classificacao DESC
        LIMIT 5
        ''', (produto_asin,))
        
        print("Top 5 melhores avaliações:")
        for review in cursor.fetchall():
            print(f"Cliente: {review[0]}, Classificação: {review[1]}, Votos: {review[2]}, Útil: {review[3]}, Data: {review[4]}")
        
        cursor.execute('''
        SELECT cliente_id, classificacao, votos, util, data
        FROM reviews
        JOIN produtos ON produtos.id = reviews.produto_id
        WHERE produtos.asin = %s
        ORDER BY util DESC, classificacao ASC
        LIMIT 5
        ''', (produto_asin,))
        
        print("\nTop 5 piores avaliações:")
        for review in cursor.fetchall():
            print(f"Cliente: {review[0]}, Classificação: {review[1]}, Votos: {review[2]}, Útil: {review[3]}, Data: {review[4]}")
    
    except Exception as e:
        print(f"Erro ao executar consulta: {e}")
    finally:
        cursor.close()
        conn.close()

def listar_similares_maiores_vendas(produto_asin):
    conn = conectar_banco()
    if conn is None:
        return
    cursor = conn.cursor()

    try:
        cursor.execute('''
        SELECT p2.titulo, p2.salesrank
        FROM produtos p1
        JOIN produtos p2 ON p1.similar = p2.asin
        WHERE p1.asin = %s AND p2.salesrank < p1.salesrank
        ORDER BY p2.salesrank ASC
        ''', (produto_asin,))
        
        print("\nProdutos similares com maiores vendas:")
        for produto in cursor.fetchall():
            print(f"Produto: {produto[0]}, Salesrank: {produto[1]}")
    
    except Exception as e:
        print(f"Erro ao executar consulta: {e}")
    finally:
        cursor.close()
        conn.close()

def evolucao_diaria_avaliacoes(produto_asin):
    conn = conectar_banco()
    if conn is None:
        return
    cursor = conn.cursor()

    try:
        cursor.execute('''
        SELECT data, AVG(classificacao) AS media_classificacao
        FROM reviews
        JOIN produtos ON produtos.id = reviews.produto_id
        WHERE produtos.asin = %s
        GROUP BY data
        ORDER BY data
        ''', (produto_asin,))
        
        print("\nEvolução diária das médias de avaliação:")
        for data, media in cursor.fetchall():
            print(f"Data: {data}, Média de Classificação: {media:.2f}")
    
    except Exception as e:
        print(f"Erro ao executar consulta: {e}")
    finally:
        cursor.close()
        conn.close()


def main():
    produto_asin = input("Digite o ASIN do produto: ")
    
    listar_top_reviews(produto_asin)
    
    listar_similares_maiores_vendas(produto_asin)

    evolucao_diaria_avaliacoes(produto_asin)
    
if __name__ == "__main__":
    main()
