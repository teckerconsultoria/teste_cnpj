#!/usr/bin/env python3
# identificar_tabelas.py - Script para identificar a estrutura das tabelas
import sqlite3
import argparse
import pprint

def analisar_tabelas(db_path):
    """Analisa a estrutura das tabelas no banco de dados"""
    print(f"Analisando estrutura do banco {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Listar todas as tabelas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = [t[0] for t in cursor.fetchall()]
    print(f"Tabelas encontradas: {tabelas}")
    
    # 2. Analisar cada tabela importante
    tabelas_importantes = ['socios', 'empresas', 'estabelecimentos', 'outros']
    estrutura = {}
    
    for tabela in tabelas:
        if tabela in tabelas_importantes or tabela.lower() in [t.lower() for t in tabelas_importantes]:
            print(f"\nAnalisando tabela: {tabela}")
            
            # Obter informações sobre as colunas
            cursor.execute(f"PRAGMA table_info({tabela})")
            colunas = cursor.fetchall()
            
            # Formatar informações das colunas
            colunas_info = []
            for col in colunas:
                colunas_info.append({
                    'index': col[0],
                    'name': col[1],
                    'type': col[2],
                })
            
            # Obter amostra de dados
            cursor.execute(f"SELECT * FROM {tabela} LIMIT 1")
            amostra = cursor.fetchone()
            
            # Mapear colunas com valores
            amostra_formatada = {}
            if amostra:
                for i, valor in enumerate(amostra):
                    if i < len(colunas_info):
                        col_name = colunas_info[i]['name']
                        amostra_formatada[col_name] = valor
            
            # Guardar informações da tabela
            estrutura[tabela] = {
                'colunas': colunas_info,
                'amostra': amostra_formatada
            }
            
            # Mostrar informações
            print(f"  Colunas: {len(colunas_info)}")
            for i, col in enumerate(colunas_info):
                if i < 10:  # Limitar para não ficar muito grande
                    print(f"    {col['index']}. {col['name']} ({col['type']})")
                elif i == 10:
                    print(f"    ... e mais {len(colunas_info)-10} colunas")
            
            print("  Amostra dos primeiros 5 campos:")
            amostra_keys = list(amostra_formatada.keys())[:5]
            for key in amostra_keys:
                print(f"    {key}: {amostra_formatada[key]}")
    
    # 3. Verificar colunas específicas que podem conter nome da empresa
    print("\nProcurando colunas que podem conter nome da empresa:")
    for tabela, info in estrutura.items():
        colunas_candidatas = []
        
        for col in info['colunas']:
            nome_col = col['name'].lower()
            # Critérios para identificar colunas que podem ter nome de empresa
            if (
                'nome' in nome_col or 
                'razao' in nome_col or 
                'social' in nome_col or 
                'empresa' in nome_col or
                nome_col == 'col_1'  # Comumente a segunda coluna tem o nome
            ):
                valor = info['amostra'].get(col['name'], 'N/A')
                colunas_candidatas.append((col['name'], valor))
        
        if colunas_candidatas:
            print(f"\nTabela {tabela} - colunas candidatas:")
            for nome_col, valor in colunas_candidatas:
                print(f"  {nome_col}: {valor}")
    
    # 4. Verificar colunas que podem conter a situação cadastral
    print("\nProcurando colunas que podem conter situação cadastral:")
    for tabela, info in estrutura.items():
        colunas_candidatas = []
        
        for col in info['colunas']:
            nome_col = col['name'].lower()
            # Critérios para identificar colunas com situação cadastral
            if (
                'situacao' in nome_col or 
                'status' in nome_col or 
                nome_col == '02' or  # Comumente a situação cadastral
                nome_col == 'col_2'
            ):
                valor = info['amostra'].get(col['name'], 'N/A')
                colunas_candidatas.append((col['name'], valor))
        
        if colunas_candidatas:
            print(f"\nTabela {tabela} - colunas candidatas para situação:")
            for nome_col, valor in colunas_candidatas:
                print(f"  {nome_col}: {valor}")
    
    # 5. Verificar colunas relacionadas ao endereço
    print("\nProcurando colunas que podem conter endereço:")
    for tabela, info in estrutura.items():
        colunas_candidatas = []
        
        for col in info['colunas']:
            nome_col = col['name'].lower()
            # Critérios para identificar colunas com endereço
            if (
                'endereco' in nome_col or
                'logradouro' in nome_col or
                'rua' in nome_col or
                'bairro' in nome_col or
                'cidade' in nome_col or
                'uf' in nome_col or
                'cep' in nome_col
            ):
                valor = info['amostra'].get(col['name'], 'N/A')
                colunas_candidatas.append((col['name'], valor))
        
        if colunas_candidatas:
            print(f"\nTabela {tabela} - colunas candidatas para endereço:")
            for nome_col, valor in colunas_candidatas:
                print(f"  {nome_col}: {valor}")
    
    # 6. Verificar colunas para CNPJ básico (chave de relacionamento)
    print("\nProcurando colunas que podem conter CNPJ básico:")
    for tabela, info in estrutura.items():
        colunas_candidatas = []
        
        for col in info['colunas']:
            nome_col = col['name'].lower()
            # Critérios para identificar colunas com CNPJ básico
            if (
                'cnpj' in nome_col or 
                'basico' in nome_col or 
                nome_col == 'col_0' or  # Comumente a primeira coluna é o CNPJ
                nome_col == '03769328'  # Específico para este caso
            ):
                valor = info['amostra'].get(col['name'], 'N/A')
                colunas_candidatas.append((col['name'], valor))
        
        if colunas_candidatas:
            print(f"\nTabela {tabela} - colunas candidatas para CNPJ básico:")
            for nome_col, valor in colunas_candidatas:
                print(f"  {nome_col}: {valor}")
    
    conn.close()
    print("\nAnálise concluída!")

def testar_consulta_especifica(db_path, cnpj_basico="3769339"):
    """Testa consultas específicas para encontrar o nome da empresa"""
    print(f"Testando consultas para CNPJ básico {cnpj_basico}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Testar diferentes tabelas e consultas
    tabelas_possiveis = [
        "empresas", 
        "outros", 
        "k3241k03200y0d",  # Possível nome alternativo
        "estabelecimentos"
    ]
    
    for tabela in tabelas_possiveis:
        try:
            print(f"\nTestando consulta na tabela {tabela}:")
            # Primeiro verificar se a tabela existe
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tabela,))
            if not cursor.fetchone():
                print(f"  Tabela {tabela} não existe")
                continue
            
            # Verificar as colunas da tabela
            cursor.execute(f"PRAGMA table_info({tabela})")
            colunas = cursor.fetchall()
            print(f"  Colunas na tabela: {[col[1] for col in colunas]}")
            
            # Tentar uma consulta simples
            cursor.execute(f"SELECT * FROM {tabela} LIMIT 1")
            amostra = cursor.fetchone()
            if amostra:
                print(f"  Amostra: {amostra[:5]}...")  # Mostrar apenas primeiros 5 valores
            
            # Tentar buscar pelo CNPJ específico
            try:
                # Identificar a coluna que pode ter o CNPJ
                col_cnpj = None
                for col in colunas:
                    col_name = col[1].lower()
                    if (
                        'cnpj' in col_name or 
                        'basico' in col_name or 
                        col_name == 'col_0' or
                        col_name == '03769328'
                    ):
                        col_cnpj = col[1]
                        break
                
                if col_cnpj:
                    query = f"SELECT * FROM {tabela} WHERE {col_cnpj} = ? LIMIT 1"
                    print(f"  Executando: {query} com valor {cnpj_basico}")
                    cursor.execute(query, (cnpj_basico,))
                    resultado = cursor.fetchone()
                    
                    if resultado:
                        print(f"  ✓ Registro encontrado!")
                        # Mostrar todos os valores
                        for i, col in enumerate(colunas):
                            if i < len(resultado):
                                print(f"    {col[1]}: {resultado[i]}")
                    else:
                        print(f"  ✗ Nenhum registro encontrado")
                else:
                    print(f"  ✗ Não foi possível identificar coluna para CNPJ")
            except Exception as e:
                print(f"  Erro na consulta por CNPJ: {e}")
            
        except Exception as e:
            print(f"  Erro ao testar tabela {tabela}: {e}")
    
    conn.close()
    print("\nTestes de consulta concluídos!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Identifica a estrutura das tabelas no banco')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--cnpj', type=str, default='3769339', help='CNPJ básico para teste de consulta')
    parser.add_argument('--consulta', action='store_true', help='Executar teste de consulta')
    
    args = parser.parse_args()
    
    analisar_tabelas(args.banco)
    
    if args.consulta:
        testar_consulta_especifica(args.banco, args.cnpj)
