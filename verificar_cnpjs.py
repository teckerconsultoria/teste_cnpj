#!/usr/bin/env python3
# verificar_cnpjs.py - Script para verificar CNPJs ativos e seus sócios
import sqlite3
import argparse
import pandas as pd
import os
import csv
from tqdm import tqdm

def carregar_cnpjs_do_arquivo(arquivo):
    """
    Carrega uma lista de CNPJs de um arquivo (CSV ou TXT)
    """
    cnpjs = []
    
    if not os.path.exists(arquivo):
        print(f"Erro: Arquivo {arquivo} não encontrado.")
        return []
    
    # Tenta determinar o tipo de arquivo pela extensão
    extensao = os.path.splitext(arquivo)[1].lower()
    
    try:
        if extensao == '.csv':
            # Tentar diferentes delimitadores
            delimitadores = [',', ';', '\t', '|']
            for delimitador in delimitadores:
                try:
                    df = pd.read_csv(arquivo, delimiter=delimitador)
                    
                    # Procurar coluna com CNPJs
                    for coluna in df.columns:
                        if 'cnpj' in coluna.lower():
                            cnpjs = df[coluna].astype(str).tolist()
                            print(f"Encontrados {len(cnpjs)} CNPJs na coluna '{coluna}'")
                            break
                    
                    if not cnpjs and len(df.columns) > 0:
                        # Se não encontrou coluna específica, usa a primeira
                        cnpjs = df.iloc[:, 0].astype(str).tolist()
                        print(f"Usando primeira coluna com {len(cnpjs)} valores")
                    
                    if cnpjs:
                        break
                except:
                    continue
            
            if not cnpjs:
                raise Exception("Não foi possível identificar CNPJs no arquivo CSV")
        
        elif extensao == '.txt':
            # Assume um CNPJ por linha
            with open(arquivo, 'r') as f:
                linhas = f.readlines()
            
            for linha in linhas:
                # Limpa a linha e procura por um padrão de CNPJ (apenas dígitos)
                cnpj = ''.join(c for c in linha if c.isdigit())
                if cnpj:
                    cnpjs.append(cnpj)
            
            print(f"Encontrados {len(cnpjs)} CNPJs no arquivo de texto")
        
        else:
            # Tenta como texto simples
            with open(arquivo, 'r') as f:
                conteudo = f.read()
            
            # Procura por padrões de CNPJ (sequências de dígitos)
            import re
            cnpjs = re.findall(r'\d{8,14}', conteudo)
            print(f"Encontrados {len(cnpjs)} possíveis CNPJs no arquivo")
    
    except Exception as e:
        print(f"Erro ao carregar arquivo: {e}")
        return []
    
    # Limpa e padroniza os CNPJs
    cnpjs_limpos = []
    for cnpj in cnpjs:
        # Remove caracteres não numéricos
        cnpj_limpo = ''.join(c for c in str(cnpj) if c.isdigit())
        
        # Verifica se tem pelo menos 8 dígitos (CNPJ básico)
        if len(cnpj_limpo) >= 8:
            # Para CNPJs incompletos (apenas o básico), considera os 8 primeiros dígitos
            if len(cnpj_limpo) < 14:
                cnpj_limpo = cnpj_limpo[:8].zfill(8)
            else:
                # Para CNPJs completos, considera os 14 dígitos
                cnpj_limpo = cnpj_limpo[:14].zfill(14)
            
            cnpjs_limpos.append(cnpj_limpo)
    
    # Remove duplicatas
    cnpjs_limpos = list(set(cnpjs_limpos))
    print(f"Total de {len(cnpjs_limpos)} CNPJs únicos para consulta")
    
    return cnpjs_limpos

def verificar_cnpjs(db_path, cnpjs, saida=None):
    """
    Verifica se os CNPJs estão ativos e quem são seus sócios
    """
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados {db_path} não encontrado.")
        return False
    
    # Conectar ao banco
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verificar se as visões necessárias existem
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_cnpj_status'")
    tem_view_status = cursor.fetchone() is not None
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='vw_cnpj_socios'")
    tem_view_socios = cursor.fetchone() is not None
    
    if not tem_view_status or not tem_view_socios:
        print("As visões necessárias não existem no banco. Execute o script de correção primeiro.")
        
        # Procurar tabela de estabelecimentos
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='estabelecimentos'")
        tem_tabela_estab = cursor.fetchone() is not None
        
        if tem_tabela_estab:
            # Tentar criar visões simplificadas
            try:
                print("Criando visões simplificadas...")
                cursor.execute("""
                CREATE VIEW IF NOT EXISTS vw_cnpj_status AS
                SELECT
                    cnpj_basico,
                    "02" AS situacao_cadastral
                FROM estabelecimentos
                """)
                
                cursor.execute("""
                CREATE VIEW IF NOT EXISTS vw_cnpj_socios AS
                SELECT
                    e.cnpj_basico,
                    s."livia_maria_andrade_ramos_gaertner" AS nome_socio,
                    s."***331355**" AS cpf_cnpj_socio
                FROM estabelecimentos e
                LEFT JOIN socios s ON e.cnpj_basico = s."03769328"
                """)
                
                conn.commit()
                print("Visões simplificadas criadas com sucesso.")
            except Exception as e:
                print(f"Erro ao criar visões: {e}")
                conn.close()
                return False
    
    # Preparar arquivo de saída
    if saida:
        arquivo_saida = saida
    else:
        arquivo_saida = "resultados_cnpj.csv"
    
    # Preparar cabeçalho do CSV
    cabecalho = ['CNPJ', 'Situação', 'Descrição', 'QTD_Socios', 'Socios_CPF_Nome']
    resultados = []
    
    print(f"Verificando {len(cnpjs)} CNPJs...")
    
    for cnpj in tqdm(cnpjs):
        try:
            # Determinar se é CNPJ completo ou básico
            if len(cnpj) == 14:
                cnpj_basico = cnpj[:8]
            else:
                cnpj_basico = cnpj
            
            # Consultar situação do CNPJ
            cursor.execute(f"""
            SELECT 
                cnpj_completo, 
                situacao_cadastral, 
                CASE situacao_cadastral
                    WHEN '1' THEN 'NULA'
                    WHEN '2' THEN 'ATIVA'
                    WHEN '3' THEN 'SUSPENSA'
                    WHEN '4' THEN 'INAPTA'
                    WHEN '8' THEN 'BAIXADA'
                    ELSE 'DESCONHECIDA'
                END AS situacao_descricao
            FROM vw_cnpj_status
            WHERE cnpj_basico = ?
            LIMIT 1
            """, (cnpj_basico,))
            
            status_result = cursor.fetchone()
            
            if not status_result:
                # CNPJ não encontrado
                resultados.append([
                    cnpj,
                    "NÃO ENCONTRADO",
                    "CNPJ não consta na base",
                    0,
                    ""
                ])
                continue
            
            cnpj_completo, situacao_cadastral, situacao_descricao = status_result
            
            # Consultar sócios do CNPJ
            cursor.execute(f"""
            SELECT 
                nome_socio,
                cpf_cnpj_socio
            FROM vw_cnpj_socios
            WHERE cnpj_basico = ?
            """, (cnpj_basico,))
            
            socios = cursor.fetchall()
            
            # Formatar lista de sócios
            socios_texto = ""
            if socios:
                socios_lista = []
                for socio in socios:
                    nome_socio, cpf_socio = socio
                    socios_lista.append(f"{cpf_socio} - {nome_socio}")
                
                socios_texto = " | ".join(socios_lista)
            
            # Adicionar ao resultado
            resultados.append([
                cnpj_completo if cnpj_completo else cnpj,
                situacao_cadastral,
                situacao_descricao,
                len(socios),
                socios_texto
            ])
            
        except Exception as e:
            print(f"Erro ao verificar CNPJ {cnpj}: {e}")
            resultados.append([cnpj, "ERRO", str(e), 0, ""])
    
    # Salvar resultados
    try:
        with open(arquivo_saida, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(cabecalho)
            writer.writerows(resultados)
        
        print(f"Resultados salvos em {arquivo_saida}")
    except Exception as e:
        print(f"Erro ao salvar resultados: {e}")
    
    conn.close()
    
    # Resumo dos resultados
    try:
        df_resultados = pd.DataFrame(resultados, columns=cabecalho)
        
        # Contagem por situação
        contagem_situacao = df_resultados['Situação'].value_counts()
        
        print("\n=== RESUMO DOS RESULTADOS ===")
        print(f"Total de CNPJs consultados: {len(cnpjs)}")
        print("Situação cadastral:")
        for situacao, contagem in contagem_situacao.items():
            print(f"  - {situacao}: {contagem}")
        
        # Quantidade média de sócios
        media_socios = df_resultados['QTD_Socios'].mean()
        print(f"Média de sócios por CNPJ: {media_socios:.1f}")
        
        # CNPJs ativos
        ativos = df_resultados[df_resultados['Situação'] == '2'].shape[0]
        print(f"CNPJs ATIVOS: {ativos} ({ativos/len(cnpjs)*100:.1f}%)")
        
        return True
    except Exception as e:
        print(f"Erro ao gerar resumo: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Verificar CNPJs ativos e seus sócios')
    
    # Argumentos de linha de comando
    parser.add_argument('--arquivo', type=str, help='Arquivo com lista de CNPJs (CSV ou TXT)')
    parser.add_argument('--cnpj', type=str, help='CNPJ único para consulta')
    parser.add_argument('--saida', type=str, default='resultados_cnpj.csv', help='Arquivo de saída (padrão: resultados_cnpj.csv)')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados (padrão: cnpj_amostra.db)')
    
    args = parser.parse_args()
    
    # Verificar se foi fornecido um arquivo ou um CNPJ
    if not args.arquivo and not args.cnpj:
        print("Erro: Forneça um arquivo com CNPJs ou um CNPJ único")
        parser.print_help()
        return
    
    # Lista para armazenar os CNPJs
    cnpjs = []
    
    # Se foi fornecido um CNPJ único
    if args.cnpj:
        # Limpa e adiciona à lista
        cnpj_limpo = ''.join(c for c in args.cnpj if c.isdigit())
        cnpjs.append(cnpj_limpo)
    
    # Se foi fornecido um arquivo
    if args.arquivo:
        cnpjs = carregar_cnpjs_do_arquivo(args.arquivo)
    
    if not cnpjs:
        print("Erro: Nenhum CNPJ válido encontrado")
        return
    
    # Verificar os CNPJs
    verificar_cnpjs(args.banco, cnpjs, args.saida)

if __name__ == "__main__":
    main()
