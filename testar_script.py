#!/usr/bin/env python3
# corrigir_testar_desempenho.py - Versão adaptada do testar_desempenho.py
import sqlite3
import time
from difflib import SequenceMatcher
import random
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re

def normalizar_nome(nome):
    """Normaliza o nome para comparação"""
    import unicodedata
    import re
    
    if not isinstance(nome, str):
        return ""
    
    # Remover acentos
    nome = unicodedata.normalize('NFKD', nome)
    nome = nome.encode('ascii', errors='ignore').decode('ascii')
    
    # Converter para maiúsculas
    nome = nome.upper()
    
    # Remover caracteres especiais e números
    nome = re.sub(r'[^A-Z ]', '', nome)
    
    # Remover espaços extras
    nome = ' '.join(nome.split())
    
    return nome

def similaridade(a, b):
    """Calcula a similaridade entre duas strings"""
    if not a or not b:
        return 0
    return SequenceMatcher(None, a, b).ratio()

def consultar_por_miolo_cpf(conn, miolo_cpf, nome=None, limiar_similaridade=0.7):
    """
    Consulta o banco pelo miolo do CPF e, opcionalmente, 
    valida com o nome - Versão adaptada para funcionar com qualquer estrutura de tabela
    """
    cursor = conn.cursor()
    
    # Verificar se existe a visão otimizada (criada pelo diagnóstico)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_socios_otimizada'")
    tem_visao_otimizada = cursor.fetchone() is not None
    
    # Verificar visões existentes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    visoes = [v[0] for v in cursor.fetchall()]
    print(f"Visões disponíveis: {visoes}")
    
    # Verificar tabelas existentes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = [t[0] for t in cursor.fetchall()]
    
    # Definir tabela para consulta
    tabela_socios = None
    for tabela in tabelas:
        if tabela.lower() == 'socios':
            tabela_socios = tabela
            break
    
    # Se não encontrar explicitamente, procurar por nomes alternativos
    if not tabela_socios:
        for tabela in tabelas:
            if 'soc' in tabela.lower() or 'k3241' in tabela.lower():
                tabela_socios = tabela
                print(f"Usando tabela: {tabela_socios}")
                break
    
    if not tabela_socios:
        print("Erro: Não foi possível identificar a tabela de sócios.")
        return None, 0, 0
    
    # Obter informações sobre as colunas
    cursor.execute(f"PRAGMA table_info({tabela_socios})")
    colunas = cursor.fetchall()
    colunas_nomes = [col[1].lower() for col in colunas]
    
    # Verificar se a coluna cpf_miolo existe
    if 'cpf_miolo' not in colunas_nomes:
        print("Erro: Coluna cpf_miolo não encontrada na tabela de sócios.")
        return None, 0, 0
    
    # Identificar coluna de nome do sócio
    col_nome = None
    for possivel_nome in ['nome_socio', 'nome', 'col_2']:
        if possivel_nome in colunas_nomes:
            col_nome = possivel_nome
            break
    
    if not col_nome:
        # Usar a terceira coluna (geralmente é o nome)
        if len(colunas) >= 3:
            col_nome = colunas[2][1]
        else:
            print("Erro: Não foi possível identificar coluna com nome do sócio.")
            return None, 0, 0
    
    # Iniciar tempo de execução
    inicio = time.time()
    
    try:
        # Tentar consulta através da visão otimizada
        if tem_visao_otimizada:
            query = """
            SELECT 
                cpf_miolo, 
                nome_socio, 
                cpf_cnpj_socio, 
                cnpj_basico
            FROM vw_socios_otimizada
            WHERE cpf_miolo = ?
            LIMIT 100
            """
        # Se não tiver a visão, consultar diretamente na tabela
        else:
            query = f"""
            SELECT 
                cpf_miolo, 
                {col_nome} as nome_socio, 
                * 
            FROM {tabela_socios}
            WHERE cpf_miolo = ?
            LIMIT 100
            """
        
        cursor.execute(query, (miolo_cpf,))
        resultados = cursor.fetchall()
        
        # Tempo de execução da consulta principal
        tempo_query = time.time() - inicio
        
        # Se não encontrou, retorna vazio
        if not resultados:
            return None, tempo_query, 0
        
        # Se não foi fornecido nome, retorna todos os resultados
        if not nome:
            # Converter para formato padronizado
            resultados_formatados = []
            for resultado in resultados:
                # Criar dicionário a partir do resultado
                res_dict = {}
                for i, col in enumerate(cursor.description):
                    res_dict[col[0]] = resultado[i]
                
                # Adicionar score 1.0 para padronizar saída
                res_dict['score'] = 1.0
                
                resultados_formatados.append(res_dict)
            
            return resultados_formatados[0], tempo_query, len(resultados)
        
        # Filtrar resultados por similaridade de nome
        nome_normalizado = normalizar_nome(nome)
        
        resultados_com_score = []
        tempo_inicio_processamento = time.time()
        
        for resultado in resultados:
            # Criar dicionário com os resultados
            res_dict = {}
            for i, col in enumerate(cursor.description):
                res_dict[col[0]] = resultado[i]
            
            # Calcular similaridade do nome
            nome_socio = resultado[1]  # Posição 1 deve ser o nome_socio (definido na query)
            nome_socio_norm = normalizar_nome(nome_socio) if nome_socio else ""
            score = similaridade(nome_normalizado, nome_socio_norm)
            
            res_dict['score'] = score
            
            resultados_com_score.append(res_dict)
        
        # Ordenar por similaridade
        resultados_com_score.sort(key=lambda x: x['score'], reverse=True)
        
        tempo_processamento = time.time() - tempo_inicio_processamento
        tempo_total = tempo_query + tempo_processamento
        
        # Retornar o melhor resultado se tiver uma similaridade mínima
        if resultados_com_score and resultados_com_score[0]['score'] > limiar_similaridade:
            return resultados_com_score[0], tempo_total, len(resultados)
        
        return None, tempo_total, len(resultados)
    
    except Exception as e:
        print(f"Erro na consulta: {e}")
        return None, 0, 0

def preparar_banco(conn):
    """
    Preparar o banco para testes, garantindo que as visões necessárias existem
    """
    cursor = conn.cursor()
    
    # Verificar se a visão otimizada já existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_socios_otimizada'")
    if cursor.fetchone():
        print("Visão vw_socios_otimizada já existe.")
        return
    
    # Verificar tabelas disponíveis
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = [t[0] for t in cursor.fetchall()]
    
    # Definir tabela para consulta
    tabela_socios = None
    for tabela in tabelas:
        if tabela.lower() == 'socios':
            tabela_socios = tabela
            break
    
    # Se não encontrar explicitamente, procurar por nomes alternativos
    if not tabela_socios:
        for tabela in tabelas:
            if 'soc' in tabela.lower() or 'k3241' in tabela.lower():
                tabela_socios = tabela
                print(f"Usando tabela: {tabela_socios}")
                break
    
    if not tabela_socios:
        print("Erro: Não foi possível identificar a tabela de sócios.")
        return
    
    try:
        # Verificar colunas da tabela
        cursor.execute(f"PRAGMA table_info({tabela_socios})")
        colunas = cursor.fetchall()
        colunas_nomes = [col[1].lower() for col in colunas]
        
        # Verificar se a coluna cpf_miolo existe
        if 'cpf_miolo' not in colunas_nomes:
            print("Erro: Coluna cpf_miolo não encontrada. Execute o script corrigir_processamento.py primeiro.")
            return
        
        # Identificar coluna de nome do sócio
        col_nome = None
        for possivel_nome in ['nome_socio', 'nome', 'col_2']:
            if possivel_nome in colunas_nomes:
                col_nome = possivel_nome
                break
        
        if not col_nome:
            # Usar a terceira coluna (geralmente é o nome)
            if len(colunas) >= 3:
                col_nome = colunas[2][1]
            else:
                print("Erro: Não foi possível identificar coluna com nome do sócio.")
                return
        
        # Identificar coluna de CPF
        col_cpf = None
        for possivel_cpf in ['cpf_cnpj_socio', 'cpf', 'col_3']:
            if possivel_cpf in colunas_nomes:
                col_cpf = possivel_cpf
                break
        
        if not col_cpf:
            # Usar a quarta coluna (geralmente é o CPF)
            if len(colunas) >= 4:
                col_cpf = colunas[3][1]
            else:
                print("Erro: Não foi possível identificar coluna com CPF do sócio.")
                return
        
        # Identificar coluna de CNPJ básico
        col_cnpj_basico = None
        for possivel_cnpj in ['cnpj_basico', 'cnpj', 'col_0', 'col_1']:
            if possivel_cnpj in colunas_nomes:
                col_cnpj_basico = possivel_cnpj
                break
        
        if not col_cnpj_basico:
            # Usar a primeira coluna (geralmente é o CNPJ básico)
            if len(colunas) >= 1:
                col_cnpj_basico = colunas[0][1]
            else:
                print("Erro: Não foi possível identificar coluna com CNPJ básico.")
                return
        
        # Criar visão otimizada para consultas
        print("Criando visão otimizada para consultas...")
        cursor.execute(f"""
        CREATE VIEW IF NOT EXISTS vw_socios_otimizada AS
        SELECT
            cpf_miolo,
            {col_nome} AS nome_socio,
            {col_cpf} AS cpf_cnpj_socio,
            {col_cnpj_basico} AS cnpj_basico
        FROM {tabela_socios}
        """)
        
        conn.commit()
        print("Visão otimizada criada com sucesso!")
    except Exception as e:
        print(f"Erro ao preparar banco: {e}")

def gerar_miolo_aleatorio():
    """Gera um miolo de CPF aleatório"""
    return ''.join(str(random.randint(0, 9)) for _ in range(6))

def extrair_amostra_banco(conn, tamanho=100):
    """
    Extrai uma amostra de miolos e nomes do banco
    Versão adaptada para funcionar com qualquer estrutura de tabela
    """
    cursor = conn.cursor()
    
    # Verificar se existe a visão otimizada
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_socios_otimizada'")
    tem_visao_otimizada = cursor.fetchone() is not None
    
    try:
        if tem_visao_otimizada:
            # Usar a visão otimizada
            query = """
            SELECT cpf_miolo, nome_socio 
            FROM vw_socios_otimizada 
            WHERE cpf_miolo != '' AND cpf_miolo IS NOT NULL
            ORDER BY RANDOM()
            LIMIT ?
            """
            
            cursor.execute(query, (tamanho,))
            resultados = cursor.fetchall()
            
            if resultados:
                return resultados
        
        # Se não tiver visão ou não retornou resultados, tentar diretamente na tabela
        # Verificar tabelas existentes
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas = [t[0] for t in cursor.fetchall()]
        
        # Definir tabela para consulta
        tabela_socios = None
        for tabela in tabelas:
            if tabela.lower() == 'socios':
                tabela_socios = tabela
                break
        
        if not tabela_socios:
            for tabela in tabelas:
                if 'soc' in tabela.lower() or 'k3241' in tabela.lower():
                    tabela_socios = tabela
                    print(f"Usando tabela: {tabela_socios}")
                    break
        
        if not tabela_socios:
            print("Erro: Não foi possível identificar a tabela de sócios.")
            return []
        
        # Verificar colunas da tabela
        cursor.execute(f"PRAGMA table_info({tabela_socios})")
        colunas = cursor.fetchall()
        colunas_nomes = [col[1].lower() for col in colunas]
        
        # Verificar se tem a coluna cpf_miolo
        if 'cpf_miolo' not in colunas_nomes:
            print("Erro: Coluna cpf_miolo não encontrada. Execute o script corrigir_processamento.py primeiro.")
            return []
        
        # Identificar coluna de nome do sócio
        col_nome = None
        for possivel_nome in ['nome_socio', 'nome', 'col_2']:
            if possivel_nome.lower() in colunas_nomes:
                col_nome = possivel_nome
                break
        
        if not col_nome:
            # Usar a terceira coluna (geralmente é o nome)
            if len(colunas) >= 3:
                col_nome = colunas[2][1]
            else:
                print("Erro: Não foi possível identificar coluna com nome do sócio.")
                return []
        
        print(f"Consultando diretamente na tabela {tabela_socios}, coluna nome: {col_nome}")
        
        # Consultar diretamente na tabela
        query = f"""
        SELECT cpf_miolo, {col_nome} 
        FROM {tabela_socios} 
        WHERE cpf_miolo != '' AND cpf_miolo IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
        """
        
        cursor.execute(query, (tamanho,))
        resultados = cursor.fetchall()
        
        if not resultados:
            print("Nenhum resultado encontrado com miolo não vazio. Gerando amostra aleatória...")
            
            # Buscar apenas nomes para combinar com miolos aleatórios
            cursor.execute(f"SELECT {col_nome} FROM {tabela_socios} WHERE {col_nome} IS NOT NULL AND {col_nome} != '' ORDER BY RANDOM() LIMIT ?", (tamanho,))
            nomes = [row[0] for row in cursor.fetchall()]
            
            if not nomes:
                print("Não foram encontrados nomes. Gerando dados completamente aleatórios.")
                nomes = [f"NOME ALEATORIO {i}" for i in range(tamanho)]
            
            miolos = [gerar_miolo_aleatorio() for _ in range(len(nomes))]
            resultados = list(zip(miolos, nomes))
        
        return resultados
    except Exception as e:
        print(f"Erro ao extrair amostra: {e}")
        return []

def gerar_graficos(tempos_dados_reais, tempos_aleatorios, scores, resultados_encontrados):
    """Gera gráficos de análise de desempenho"""
    # Criar diretório para os gráficos
    os.makedirs("resultados", exist_ok=True)
    
    # Configuração geral dos gráficos
    sns.set_theme(style="whitegrid")
    
    # 1. Histograma dos tempos de resposta
    plt.figure(figsize=(10, 6))
    plt.hist(
        [t*1000 for t in tempos_dados_reais], 
        bins=20, 
        alpha=0.7, 
        label='Consultas com dados reais'
    )
    plt.hist(
        [t*1000 for t in tempos_aleatorios], 
        bins=20, 
        alpha=0.7, 
        label='Consultas com dados aleatórios'
    )
    plt.xlabel('Tempo de resposta (ms)')
    plt.ylabel('Frequência')
    plt.title('Distribuição dos tempos de resposta')
    plt.legend()
    plt.savefig('resultados/tempos_resposta.png')
    
    # 2. Gráfico de scores de similaridade
    if scores:
        plt.figure(figsize=(10, 6))
        plt.hist(scores, bins=10, range=(0, 1))
        plt.xlabel('Score de similaridade')
        plt.ylabel('Frequência')
        plt.title('Distribuição dos scores de similaridade')
        plt.savefig('resultados/similaridade.png')
    
    # 3. Relação entre número de resultados e tempo de resposta
    plt.figure(figsize=(10, 6))
    df = pd.DataFrame({
        'Número de resultados': resultados_encontrados,
        'Tempo (ms)': [t*1000 for t in tempos_dados_reais + tempos_aleatorios]
    })
    # Remover outliers para melhor visualização
    if not df.empty and len(df) > 5:
        df = df[df['Tempo (ms)'] < df['Tempo (ms)'].quantile(0.95)]
    
    sns.scatterplot(data=df, x='Número de resultados', y='Tempo (ms)')
    plt.title('Relação entre número de resultados e tempo de resposta')
    plt.savefig('resultados/resultados_vs_tempo.png')
    
    print(f"Gráficos salvos no diretório 'resultados'")

def apresentar_resultado_detalhado(resultado):
    """Apresenta um resultado detalhado com dados de empresas"""
    if not resultado:
        return "Não encontrado"
    
    # Verificar quais campos estão disponíveis
    campos_disponiveis = resultado.keys()
    
    # Formatar a saída com base nos campos disponíveis
    linhas = []
    linhas.append(f"Sócio: {resultado.get('nome_socio', '')}")
    
    if 'cpf_cnpj_socio' in campos_disponiveis:
        linhas.append(f"CPF/CNPJ: {resultado.get('cpf_cnpj_socio', '')}")
    
    if 'cpf_miolo' in campos_disponiveis:
        linhas.append(f"Miolo CPF: {resultado.get('cpf_miolo', '')}")
    
    if 'cnpj_basico' in campos_disponiveis:
        linhas.append(f"CNPJ Básico: {resultado.get('cnpj_basico', '')}")
    
    if 'score' in campos_disponiveis:
        linhas.append(f"Score: {resultado.get('score', 0):.2f}")
    
    return "\n  ".join(linhas)

def testar_desempenho(num_consultas=100, use_graficos=True):
    """
    Teste de desempenho com consultas no banco
    """
    db_path = "cnpj_amostra.db"
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados {db_path} não encontrado.")
        return
    
    conn = sqlite3.connect(db_path)
    
    print(f"Realizando {num_consultas} consultas de teste...")
    
    # Verificar a estrutura do banco
    print("Verificando a estrutura do banco de dados...")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tabelas = cursor.fetchall()
    print(f"Tabelas encontradas: {[t[0] for t in tabelas]}")
    
    # Verificar visões disponíveis
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")
    visoes = cursor.fetchall()
    print(f"Visões encontradas: {[v[0] for v in visoes]}")
    
    # Preparar o banco se necessário
    preparar_banco(conn)
    
    # Extrair amostra do banco para testes com dados reais
    print("Extraindo amostra de dados para teste...")
    amostra_real = extrair_amostra_banco(conn, tamanho=min(100, num_consultas // 2))
    
    if not amostra_real:
        print("Erro: Não foi possível extrair amostra do banco. Verifique a estrutura do banco de dados.")
        conn.close()
        return
    
    # Resultados e métricas
    tempos_dados_reais = []
    tempos_aleatorios = []
    acertos = 0
    total_consultas_reais = 0
    scores = []
    resultados_encontrados = []
    
    # Teste 1: Consultas com dados reais (validação de concordância)
    print("\nTeste 1: Consultas com dados reais (miolo + nome correspondente):")
    for miolo, nome in amostra_real:
        total_consultas_reais += 1
        resultado, tempo, num_resultados = consultar_por_miolo_cpf(conn, miolo, nome)
        tempos_dados_reais.append(tempo)
        resultados_encontrados.append(num_resultados)
        
        if resultado:
            acertos += 1
            scores.append(resultado['score'])
        
        if total_consultas_reais <= 5 or total_consultas_reais % 10 == 0:  # Mostrar apenas alguns resultados
            print(f"Miolo: {miolo}, Nome: {nome}")
            print(f"  {apresentar_resultado_detalhado(resultado)}")
            print(f"  Tempo: {tempo*1000:.2f}ms, Resultados: {num_resultados}")
    
    # Teste 2: Consultas com dados reais (somente miolo)
    print("\nTeste 2: Consultas com miolo real (sem validação de nome):")
    for miolo, _ in amostra_real[:min(5, len(amostra_real))]:
        resultado, tempo, num_resultados = consultar_por_miolo_cpf(conn, miolo)
        # Não adiciona aos tempos pois já estamos medindo apenas a busca por miolo
        
        print(f"Miolo: {miolo}")
        print(f"  {apresentar_resultado_detalhado(resultado)}")
        print(f"  Tempo: {tempo*1000:.2f}ms, Resultados: {num_resultados}")
    
    # Teste 3: Consultas com miolos aleatórios (provavelmente não encontrados)
    print("\nTeste 3: Consultas com miolos aleatórios:")
    num_aleatorios = min(20, num_consultas // 5)
    for _ in range(num_aleatorios):
        miolo = gerar_miolo_aleatorio()
        resultado, tempo, num_resultados = consultar_por_miolo_cpf(conn, miolo)
        tempos_aleatorios.append(tempo)
        resultados_encontrados.append(num_resultados)
        
        if _ < 5:  # Mostrar apenas os 5 primeiros resultados
            print(f"Miolo: {miolo}")
            print(f"  {apresentar_resultado_detalhado(resultado) if resultado else 'Não encontrado'}")
            print(f"  Tempo: {tempo*1000:.2f}ms, Resultados: {num_resultados}")
    
    # Cálculo de estatísticas
    conn.close()
    
    todos_tempos = tempos_dados_reais + tempos_aleatorios
    
    if not todos_tempos:
        print("Nenhum resultado de tempo obtido. Verifique os erros acima.")
        return
    
    tempo_medio = sum(todos_tempos) / len(todos_tempos) if todos_tempos else 0
    tempo_medio_reais = sum(tempos_dados_reais) / len(tempos_dados_reais) if tempos_dados_reais else 0
    tempo_medio_aleatorios = sum(tempos_aleatorios) / len(tempos_aleatorios) if tempos_aleatorios else 0
    
    tempo_min = min(todos_tempos) if todos_tempos else 0
    tempo_max = max(todos_tempos) if todos_tempos else 0
    
    # Gerar gráficos
    if use_graficos and tempos_dados_reais:
        try:
            gerar_graficos(tempos_dados_reais, tempos_aleatorios, scores, resultados_encontrados)
        except Exception as e:
            print(f"Erro ao gerar gráficos: {e}")
    
    # Exibir estatísticas finais
    print("\n" + "="*50)
    print("RESULTADOS DO TESTE DE DESEMPENHO:")
    print("="*50)
    print(f"Total de consultas: {len(todos_tempos)}")
    print(f"- Com dados reais: {len(tempos_dados_reais)}")
    print(f"- Com dados aleatórios: {len(tempos_aleatorios)}")
    print("\nTempos de resposta:")
    print(f"- Tempo médio geral: {tempo_medio*1000:.2f}ms")
    print(f"- Tempo médio (dados reais): {tempo_medio_reais*1000:.2f}ms")
    print(f"- Tempo médio (aleatórios): {tempo_medio_aleatorios*1000:.2f}ms")
    print(f"- Tempo mínimo: {tempo_min*1000:.2f}ms")
    print(f"- Tempo máximo: {tempo_max*1000:.2f}ms")
    
    if total_consultas_reais > 0:
        print(f"\nTaxa de acerto validação nome+miolo: {acertos/total_consultas_reais*100:.1f}%")
    print(f"Consultas por segundo (estimativa): {1/tempo_medio:.1f}")
    
    # Salvar resultados em CSV
    try:
        df_resultados = pd.DataFrame({
            'Métrica': [
                'Total de consultas',
                'Consultas com dados reais',
                'Consultas com dados aleatórios',
                'Tempo médio geral (ms)',
                'Tempo médio dados reais (ms)',
                'Tempo médio dados aleatórios (ms)',
                'Tempo mínimo (ms)',
                'Tempo máximo (ms)',
                'Taxa de acerto (%)',
                'Consultas por segundo'
            ],
            'Valor': [
                len(todos_tempos),
                len(tempos_dados_reais),
                len(tempos_aleatorios),
                tempo_medio*1000,
                tempo_medio_reais*1000,
                tempo_medio_aleatorios*1000,
                tempo_min*1000,
                tempo_max*1000,
                acertos/total_consultas_reais*100 if total_consultas_reais > 0 else 0,
                1/tempo_medio if tempo_medio > 0 else 0
            ]
        })
        
        os.makedirs("resultados", exist_ok=True)
        df_resultados.to_csv('resultados/metricas_desempenho.csv', index=False)
        print("\nResultados salvos em 'resultados/metricas_desempenho.csv'")
    except Exception as e:
        print(f"Erro ao salvar resultados em CSV: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Teste de desempenho do banco de consulta CNPJ')
    parser.add_argument('--consultas', type=int, default=100, help='Número de consultas a serem realizadas')
    parser.add_argument('--no-graficos', action='store_true', help='Desativa a geração de gráficos')
    
    args = parser.parse_args()
    
    testar_desempenho(args.consultas, not args.no_graficos)
