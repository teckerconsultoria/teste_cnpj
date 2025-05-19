#!/usr/bin/env python3
# explorar_banco.py
import sqlite3
import pandas as pd

def main():
    """Explora o conteúdo do banco de dados para identificar as colunas"""
    conn = sqlite3.connect("cnpj_amostra.db")
    
    # Listar tabelas
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = [t[0] for t in cursor.fetchall()]
    
    print(f"Tabelas no banco: {tabelas}")
    
    # Examinar a tabela de sócios
    tabela_socios = "k3241k03200y0d"  # Tabela identificada anteriormente
    
    # Obter estrutura
    cursor.execute(f"PRAGMA table_info({tabela_socios})")
    colunas = cursor.fetchall()
    print(f"\nEstrutura da tabela {tabela_socios}:")
    for col in colunas:
        print(f"  {col[0]}: {col[1]} ({col[2]})")
    
    # Obter amostra de dados
    cursor.execute(f"SELECT * FROM {tabela_socios} LIMIT 10")
    dados = cursor.fetchall()
    
    print("\nAmostra de dados:")
    for i, row in enumerate(dados):
        print(f"\nRegistro {i+1}:")
        for j, col in enumerate(row):
            print(f"  col_{j}: {col}")
    
    # Converter para DataFrame para análise mais fácil
    df = pd.read_sql(f"SELECT * FROM {tabela_socios} LIMIT 1000", conn)
    
    print("\nAnálise por coluna:")
    for col in df.columns:
        # Verificar se a coluna tem valores numéricos
        numeros = df[col].str.isnumeric() if df[col].dtype == 'object' else False
        num_count = numeros.sum() if isinstance(numeros, pd.Series) else 0
        
        # Contar strings com comprimento ~11 (possível CPF)
        len_11 = 0
        len_14 = 0
        if df[col].dtype == 'object':
            len_11 = (df[col].str.len() == 11).sum()
            len_14 = (df[col].str.len() == 14).sum()
        
        print(f"  {col}: ")
        print(f"    Tipo: {df[col].dtype}")
        print(f"    Valores únicos: {df[col].nunique()}")
        print(f"    Exemplo: {df[col].iloc[0]}")
        print(f"    Números: {num_count}")
        print(f"    Strings len=11: {len_11}")
        print(f"    Strings len=14: {len_14}")
    
    conn.close()

if __name__ == "__main__":
    main()