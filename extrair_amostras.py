#!/usr/bin/env python3
# extrair_amostras.py - Extrai dados de amostra do banco para testes
import sqlite3
import pandas as pd
import os
import random

def extrair_amostras(db_path, num_amostras=10):
    """Extrai amostras úteis para testes do banco"""
    print(f"Extraindo amostras do banco {db_path}...")
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados não encontrado: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Extrair amostra de sócios (nomes e CPFs)
    print("\n1. Extraindo amostra de sócios...")
    try:
        # Buscar na visão otimizada
        cursor.execute(f"""
        SELECT nome_socio, cpf_cnpj_socio, cpf_miolo, cnpj_basico
        FROM vw_socios_otimizada
        WHERE cpf_miolo != '' AND cpf_miolo IS NOT NULL
        ORDER BY RANDOM()
        LIMIT {num_amostras}
        """)
        
        socios = cursor.fetchall()
        if socios:
            print(f"✓ Encontrados {len(socios)} sócios")
            print("\nAMOSTRA DE SÓCIOS:")
            print("-" * 60)
            print(f"{'NOME':<30} {'CPF':<15} {'MIOLO':<10} {'CNPJ BASE':<12}")
            print("-" * 60)
            for socio in socios:
                nome, cpf, miolo, cnpj = socio
                print(f"{nome[:30]:<30} {cpf[:15]:<15} {miolo:<10} {cnpj:<12}")
            
            # Salvar em CSV
            df_socios = pd.DataFrame(socios, columns=['nome', 'cpf', 'miolo', 'cnpj_basico'])
            df_socios.to_csv('amostra_socios.csv', index=False)
            print("\nAmostra salva em 'amostra_socios.csv'")
            
            # Criar arquivo TXT para teste
            with open('amostra_socios.txt', 'w') as f:
                for socio in socios:
                    f.write(f"{socio[0]};{socio[1]}\n")
            print("Amostra salva em 'amostra_socios.txt'")
            
        else:
            print("Nenhum sócio encontrado!")
    except Exception as e:
        print(f"Erro ao extrair sócios: {e}")
    
    # 2. Extrair amostra de CNPJs ativos
    print("\n2. Extraindo amostra de CNPJs...")
    try:
        # Buscar CNPJs ativos (situação 2)
        cursor.execute(f"""
        SELECT cnpj_completo, situacao_cadastral, situacao_descricao
        FROM vw_cnpj_status
        WHERE situacao_cadastral = '2'  -- ATIVA
        ORDER BY RANDOM()
        LIMIT {num_amostras}
        """)
        
        cnpjs = cursor.fetchall()
        if cnpjs:
            print(f"✓ Encontrados {len(cnpjs)} CNPJs ativos")
            print("\nAMOSTRA DE CNPJs ATIVOS:")
            print("-" * 50)
            print(f"{'CNPJ':<20} {'SITUAÇÃO':<10} {'DESCRIÇÃO':<20}")
            print("-" * 50)
            for cnpj_info in cnpjs:
                cnpj, situacao, descricao = cnpj_info
                print(f"{cnpj:<20} {situacao:<10} {descricao:<20}")
            
            # Salvar em CSV
            df_cnpjs = pd.DataFrame(cnpjs, columns=['cnpj', 'situacao', 'descricao'])
            df_cnpjs.to_csv('amostra_cnpjs.csv', index=False)
            print("\nAmostra salva em 'amostra_cnpjs.csv'")
            
            # Criar arquivo TXT para teste
            with open('amostra_cnpjs.txt', 'w') as f:
                for cnpj_info in cnpjs:
                    f.write(f"{cnpj_info[0]}\n")
            print("Amostra salva em 'amostra_cnpjs.txt'")
            
        else:
            print("Nenhum CNPJ ativo encontrado!")
            
            # Tentar buscar qualquer CNPJ
            print("Buscando CNPJs de qualquer situação...")
            cursor.execute(f"""
            SELECT cnpj_basico, '0001' || '57' AS cnpj_complemento, '02' AS situacao
            FROM estabelecimentos
            ORDER BY RANDOM()
            LIMIT {num_amostras}
            """)
            
            cnpjs = cursor.fetchall()
            if cnpjs:
                print(f"✓ Encontrados {len(cnpjs)} CNPJs alternativos")
                print("\nAMOSTRA DE CNPJs ALTERNATIVOS:")
                print("-" * 40)
                print(f"{'CNPJ BASE':<15} {'COMPLEMENTO':<15} {'SITUAÇÃO':<10}")
                print("-" * 40)
                for cnpj_info in cnpjs:
                    cnpj_base, complemento, situacao = cnpj_info
                    print(f"{cnpj_base:<15} {complemento:<15} {situacao:<10}")
                
                # Salvar em CSV
                df_cnpjs = pd.DataFrame(cnpjs, columns=['cnpj_base', 'complemento', 'situacao'])
                df_cnpjs.to_csv('amostra_cnpjs_alt.csv', index=False)
                print("\nAmostra alternativa salva em 'amostra_cnpjs_alt.csv'")
                
                # Criar arquivo TXT para teste
                with open('amostra_cnpjs_alt.txt', 'w') as f:
                    for cnpj_info in cnpjs:
                        f.write(f"{cnpj_info[0]}\n")
                print("Amostra alternativa salva em 'amostra_cnpjs_alt.txt'")
            else:
                print("Nenhum CNPJ encontrado na tabela estabelecimentos!")
    except Exception as e:
        print(f"Erro ao extrair CNPJs: {e}")
    
    # 3. Extrair exemplos de consultas por miolo de CPF
    print("\n3. Simulando consultas por miolo de CPF...")
    try:
        # Pegar alguns miolos únicos
        cursor.execute("""
        SELECT DISTINCT cpf_miolo 
        FROM socios 
        WHERE cpf_miolo != '' AND cpf_miolo IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 5
        """)
        
        miolos = [row[0] for row in cursor.fetchall()]
        
        if miolos:
            print(f"Testando {len(miolos)} miolos de CPF...")
            
            resultados = []
            for miolo in miolos:
                # Contar quantos sócios compartilham este miolo
                cursor.execute(f"""
                SELECT COUNT(*) 
                FROM socios 
                WHERE cpf_miolo = ?
                """, (miolo,))
                
                count = cursor.fetchone()[0]
                
                # Buscar nomes correspondentes
                cursor.execute(f"""
                SELECT [livia_maria_andrade_ramos_gaertner], [***331355**]
                FROM socios 
                WHERE cpf_miolo = ?
                LIMIT 3
                """, (miolo,))
                
                nomes = cursor.fetchall()
                nomes_txt = ", ".join([f"{n[0]} ({n[1]})" for n in nomes])
                if len(nomes) < count:
                    nomes_txt += f" e mais {count - len(nomes)}..."
                
                resultados.append({
                    'miolo': miolo,
                    'total_socios': count,
                    'exemplos': nomes_txt
                })
            
            print("\nRESULTADOS DE CONSULTA POR MIOLO:")
            print("-" * 80)
            print(f"{'MIOLO':<10} {'TOTAL':<8} {'EXEMPLOS DE NOMES':<60}")
            print("-" * 80)
            for res in resultados:
                print(f"{res['miolo']:<10} {res['total_socios']:<8} {res['exemplos'][:60]}")
            
        else:
            print("Nenhum miolo de CPF encontrado para teste!")
    except Exception as e:
        print(f"Erro ao testar consultas: {e}")
    
    # 4. Gerar exemplo de comando para teste
    print("\n4. Comandos para teste:")
    try:
        # Sócios
        if 'df_socios' in locals() and not df_socios.empty:
            socio = df_socios.iloc[0]
            print(f"\nPara testar um sócio específico:")
            print(f"python testar_massa_nomes.py --nome \"{socio['nome']}\" --cpf \"{socio['cpf']}\"")
            print(f"\nPara testar um arquivo de sócios:")
            print(f"python testar_massa_nomes.py --arquivo amostra_socios.csv")
        
        # CNPJs
        if 'df_cnpjs' in locals() and not df_cnpjs.empty:
            cnpj = df_cnpjs.iloc[0]['cnpj']
            print(f"\nPara verificar um CNPJ específico:")
            print(f"python verificar_cnpjs.py --cnpj \"{cnpj}\"")
            print(f"\nPara verificar um arquivo de CNPJs:")
            print(f"python verificar_cnpjs.py --arquivo amostra_cnpjs.csv")
        elif 'df_cnpjs_alt' in locals() and not df_cnpjs_alt.empty:
            cnpj = df_cnpjs_alt.iloc[0]['cnpj_base']
            print(f"\nPara verificar um CNPJ específico (alternativo):")
            print(f"python verificar_cnpjs.py --cnpj \"{cnpj}\"")
            print(f"\nPara verificar um arquivo de CNPJs (alternativo):")
            print(f"python verificar_cnpjs.py --arquivo amostra_cnpjs_alt.csv")
    except Exception as e:
        print(f"Erro ao gerar comandos de exemplo: {e}")
    
    conn.close()
    print("\nExtração de amostras concluída!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extrai amostras para teste do banco')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--num', type=int, default=10, help='Número de amostras a extrair')
    
    args = parser.parse_args()
    
    extrair_amostras(args.banco, args.num)
