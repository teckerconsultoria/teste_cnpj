#!/usr/bin/env python3
# corrigir_etapas.py - Executa as correções no banco em etapas
import sqlite3
import os
import time

def verificar_banco(db_path):
    """Verifica a estrutura do banco e mostra informações"""
    print(f"Verificando banco de dados: {db_path}")
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados não encontrado!")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verificar tabelas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = cursor.fetchall()
    print(f"Tabelas encontradas: {[t[0] for t in tabelas]}")
    
    # Verificar visões
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    visoes = cursor.fetchall()
    print(f"Visões encontradas: {[v[0] for v in visoes]}")
    
    # Verificar tamanho do banco
    try:
        cursor.execute("SELECT COUNT(*) FROM socios")
        total_socios = cursor.fetchone()[0]
        print(f"Total de registros na tabela socios: {total_socios}")
    except:
        print("Não foi possível contar registros da tabela socios")
    
    conn.close()
    return True

def criar_visao_basica(db_path):
    """Cria a visão básica de sócios"""
    print("\n1. Criando visão básica de sócios...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Remover visão existente
    print("  Removendo visão existente se houver...")
    try:
        cursor.execute("DROP VIEW IF EXISTS vw_socios_otimizada")
        conn.commit()
    except Exception as e:
        print(f"  Aviso ao remover visão: {e}")
    
    # Criar visão básica
    print("  Criando visão otimizada...")
    try:
        # Usar colchetes para nomes de colunas problemáticos
        cursor.execute("""
        CREATE VIEW vw_socios_otimizada AS
        SELECT
            cpf_miolo,
            [livia_maria_andrade_ramos_gaertner] AS nome_socio,
            [***331355**] AS cpf_cnpj_socio,
            [03769328] AS cnpj_basico
        FROM socios
        """)
        conn.commit()
        print("  ✓ Visão criada com sucesso!")
    except Exception as e:
        print(f"  Erro ao criar visão: {e}")
    
    # Verificar se a visão foi criada
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_socios_otimizada'")
    if cursor.fetchone():
        print("  ✓ Visão confirmada no banco de dados!")
    else:
        print("  ✗ Visão não foi encontrada no banco!")
    
    conn.close()

def criar_indice_cpf_miolo(db_path):
    """Cria um índice para a coluna cpf_miolo"""
    print("\n2. Criando índice para cpf_miolo...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_socios_cpf_miolo ON socios(cpf_miolo)")
        conn.commit()
        print("  ✓ Índice criado com sucesso!")
    except Exception as e:
        print(f"  Erro ao criar índice: {e}")
    
    conn.close()

def testar_consulta_simples(db_path):
    """Testa uma consulta simples"""
    print("\n3. Testando consulta simples...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Consulta alguns registros para ver o formato dos dados
        cursor.execute("SELECT * FROM socios LIMIT 5")
        registros = cursor.fetchall()
        
        # Verificar se temos registros
        if registros:
            # Obter nomes das colunas
            colunas = [col[0] for col in cursor.description]
            print(f"  Colunas: {colunas}")
            print("  Amostra de registros:")
            for i, reg in enumerate(registros):
                print(f"  Registro {i+1}: {reg}")
        else:
            print("  Nenhum registro encontrado!")
    except Exception as e:
        print(f"  Erro na consulta: {e}")
    
    conn.close()

def atualizar_cpf_miolo(db_path, limite=1000):
    """Atualiza a coluna cpf_miolo em lotes"""
    print(f"\n4. Atualizando coluna cpf_miolo (em lotes de {limite})...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Verificar quantos registros precisam ser atualizados
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE cpf_miolo IS NULL 
           OR cpf_miolo = ''
           OR cpf_miolo LIKE '%[A-Z]%'
        """)
        total = cursor.fetchone()[0]
        print(f"  Registros para atualizar: {total}")
        
        if total == 0:
            print("  Nenhum registro precisa ser atualizado!")
            conn.close()
            return
        
        # Obter o nome da coluna que contém o CPF
        cursor.execute("PRAGMA table_info(socios)")
        colunas = cursor.fetchall()
        col_cpf = None
        for col in colunas:
            if '***' in col[1]:
                col_cpf = col[1]
                break
        
        if not col_cpf:
            print("  Erro: Não foi possível identificar a coluna de CPF!")
            conn.close()
            return
        
        print(f"  Coluna de CPF identificada: {col_cpf}")
        
        # Atualizar em lotes para não travar
        offset = 0
        atualizados = 0
        
        while atualizados < total:
            # Selecionar IDs para atualizar
            cursor.execute(f"""
            SELECT rowid FROM socios 
            WHERE cpf_miolo IS NULL 
               OR cpf_miolo = ''
               OR cpf_miolo LIKE '%[A-Z]%'
            LIMIT {limite} OFFSET {offset}
            """)
            ids = [row[0] for row in cursor.fetchall()]
            
            if not ids:
                break
            
            # Atualizar cada registro individualmente
            for id in ids:
                try:
                    # Selecionar o valor do CPF
                    cursor.execute(f"SELECT [{col_cpf}] FROM socios WHERE rowid = ?", (id,))
                    cpf = cursor.fetchone()[0]
                    
                    # Extrair o miolo (adaptado para diferentes formatos)
                    miolo = ""
                    if cpf and isinstance(cpf, str):
                        # Remover caracteres não numéricos
                        cpf_limpo = ''.join(c for c in cpf if c.isdigit())
                        
                        # Se CPF está em formato mascarado com ***
                        if '***' in cpf:
                            # Tentar extrair 6 dígitos após os asteriscos
                            digitos = ''.join(c for c in cpf if c.isdigit())
                            if len(digitos) >= 6:
                                miolo = digitos[:6]
                        # Se tem pelo menos 11 dígitos (CPF completo)
                        elif len(cpf_limpo) >= 11:
                            miolo = cpf_limpo[3:9]  # Posições 4 a 9
                        # Se tem menos dígitos mas pelo menos 6
                        elif len(cpf_limpo) >= 6:
                            miolo = cpf_limpo[:6]
                    
                    # Atualizar o miolo
                    if miolo:
                        cursor.execute("UPDATE socios SET cpf_miolo = ? WHERE rowid = ?", (miolo, id))
                    
                except Exception as e:
                    print(f"  Erro ao processar ID {id}: {e}")
            
            # Commitando as alterações do lote
            conn.commit()
            
            atualizados += len(ids)
            print(f"  Progresso: {atualizados}/{total} registros atualizados")
            
            offset += limite
    
    except Exception as e:
        print(f"  Erro ao atualizar cpf_miolo: {e}")
    
    conn.close()

def criar_visoes_cnpj(db_path):
    """Cria visões para consulta de CNPJs"""
    print("\n5. Criando visões para consulta de CNPJs...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Criar visão para CNPJs e sócios
    print("  Criando visão vw_cnpj_socios...")
    try:
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS vw_cnpj_socios AS
        SELECT
            e.cnpj_basico,
            e.[0001] || e.[57] AS cnpj_complemento,
            e.cnpj_basico || e.[0001] || e.[57] AS cnpj_completo,
            e.[02] AS situacao_cadastral,
            s.[livia_maria_andrade_ramos_gaertner] AS nome_socio,
            s.[***331355**] AS cpf_cnpj_socio,
            s.cpf_miolo
        FROM estabelecimentos e
        LEFT JOIN socios s ON e.cnpj_basico = s.[03769328]
        """)
        conn.commit()
        print("  ✓ Visão vw_cnpj_socios criada!")
    except Exception as e:
        print(f"  Erro ao criar visão vw_cnpj_socios: {e}")
    
    # Criar visão para status de CNPJs
    print("  Criando visão vw_cnpj_status...")
    try:
        cursor.execute("""
        CREATE VIEW IF NOT EXISTS vw_cnpj_status AS
        SELECT
            cnpj_basico,
            [0001] || [57] AS cnpj_complemento,
            cnpj_basico || [0001] || [57] AS cnpj_completo,
            [02] AS situacao_cadastral,
            CASE [02]
                WHEN '1' THEN 'NULA'
                WHEN '2' THEN 'ATIVA'
                WHEN '3' THEN 'SUSPENSA'
                WHEN '4' THEN 'INAPTA'
                WHEN '8' THEN 'BAIXADA'
                ELSE 'DESCONHECIDA'
            END AS situacao_descricao,
            [20210713] AS data_situacao,
            [4723700] AS cnae_principal,
            [rua] || ' ' || [nilso_braun] || ', ' || [s/n] AS endereco,
            [parque_das_palmeiras] AS bairro,
            [89803604] AS cep,
            [sc] AS uf
        FROM estabelecimentos
        """)
        conn.commit()
        print("  ✓ Visão vw_cnpj_status criada!")
    except Exception as e:
        print(f"  Erro ao criar visão vw_cnpj_status: {e}")
    
    conn.close()

def verificar_resultados(db_path):
    """Verifica os resultados finais"""
    print("\n6. Verificando resultados finais...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verificar visões criadas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    visoes = cursor.fetchall()
    print(f"  Visões disponíveis: {[v[0] for v in visoes]}")
    
    # Verificar índices criados
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indices = cursor.fetchall()
    print(f"  Índices disponíveis: {[i[0] for i in indices]}")
    
    # Testar a visão de sócios
    try:
        cursor.execute("SELECT COUNT(*) FROM vw_socios_otimizada")
        total = cursor.fetchone()[0]
        print(f"  Total de registros em vw_socios_otimizada: {total}")
        
        # Verificar amostra de registros com cpf_miolo preenchido
        cursor.execute("SELECT * FROM vw_socios_otimizada WHERE cpf_miolo != '' LIMIT 5")
        amostra = cursor.fetchall()
        if amostra:
            print("  Amostra de registros com cpf_miolo preenchido:")
            for i, reg in enumerate(amostra):
                print(f"    Registro {i+1}: {reg}")
        else:
            print("  Nenhum registro encontrado com cpf_miolo preenchido!")
    except Exception as e:
        print(f"  Erro ao testar visão: {e}")
    
    conn.close()

def corrigir_banco_em_etapas(db_path, etapa=0):
    """
    Corrige o banco em etapas
    etapa=0: executar todas as etapas
    etapa=1-6: executar apenas a etapa especificada
    """
    if not verificar_banco(db_path):
        return
    
    if etapa == 0 or etapa == 1:
        criar_visao_basica(db_path)
    
    if etapa == 0 or etapa == 2:
        criar_indice_cpf_miolo(db_path)
    
    if etapa == 0 or etapa == 3:
        testar_consulta_simples(db_path)
    
    if etapa == 0 or etapa == 4:
        atualizar_cpf_miolo(db_path)
    
    if etapa == 0 or etapa == 5:
        criar_visoes_cnpj(db_path)
    
    if etapa == 0 or etapa == 6:
        verificar_resultados(db_path)
    
    print("\nProcesso de correção concluído!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Corrige o banco de dados em etapas')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--etapa', type=int, default=0, help='Etapa a executar (0=todas, 1-6=específica)')
    parser.add_argument('--limite', type=int, default=1000, help='Tamanho do lote para atualização')
    
    args = parser.parse_args()
    
    corrigir_banco_em_etapas(args.banco, args.etapa)
