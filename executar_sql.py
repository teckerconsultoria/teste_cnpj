#!/usr/bin/env python3
# executar_sql.py - Script para executar o arquivo SQL de correção
import sqlite3
import os

def executar_script_sql(banco, arquivo_sql):
    """
    Executa um script SQL em um banco de dados SQLite
    """
    print(f"Verificando arquivos...")
    
    # Verificar se o banco existe
    if not os.path.exists(banco):
        print(f"Erro: Banco de dados {banco} não encontrado.")
        return False
    
    # Verificar se o arquivo SQL existe
    if not os.path.exists(arquivo_sql):
        print(f"Erro: Arquivo SQL {arquivo_sql} não encontrado.")
        return False
    
    print(f"Lendo arquivo SQL: {arquivo_sql}")
    
    # Ler o arquivo SQL
    try:
        with open(arquivo_sql, 'r') as f:
            script_sql = f.read()
    except Exception as e:
        print(f"Erro ao ler arquivo SQL: {e}")
        return False
    
    print(f"Conectando ao banco de dados: {banco}")
    
    # Conectar ao banco de dados
    try:
        conn = sqlite3.connect(banco)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return False
    
    # Executar o script em partes, para evitar problemas de 
    # execução com comandos complexos
    try:
        print("Executando comandos SQL...")
        
        # Dividir o script em comandos individuais
        comandos = script_sql.split(';')
        
        # Executar cada comando
        for i, comando in enumerate(comandos):
            comando = comando.strip()
            if comando:
                try:
                    cursor.execute(comando + ';')
                    conn.commit()
                except Exception as e:
                    print(f"Aviso: Erro ao executar comando {i+1}: {e}")
                    print(f"Comando problemático: {comando[:100]}...")
                    # Continua mesmo com erro em um comando específico
        
        # Verificar as mudanças
        cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
        visoes = cursor.fetchall()
        print(f"Visões no banco após execução: {[v[0] for v in visoes]}")
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
        indices = cursor.fetchall()
        print(f"Índices criados: {[i[0] for i in indices]}")
        
        # Fechar conexão
        conn.close()
        
        print("Script SQL executado com sucesso!")
        return True
    except Exception as e:
        print(f"Erro ao executar script SQL: {e}")
        try:
            conn.close()
        except:
            pass
        return False

if __name__ == "__main__":
    import argparse
    
    # Configurar argumentos de linha de comando
    parser = argparse.ArgumentParser(description='Executa um script SQL em um banco de dados SQLite')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados (default: cnpj_amostra.db)')
    parser.add_argument('--sql', type=str, default='correcao.sql', help='Caminho para o arquivo SQL (default: correcao.sql)')
    
    args = parser.parse_args()
    
    # Executar o script
    executar_script_sql(args.banco, args.sql)
