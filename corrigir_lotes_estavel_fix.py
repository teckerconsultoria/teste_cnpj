#!/usr/bin/env python3
# corrigir_lotes_estavel_fix.py - Versão corrigida para problema da tabela de controle
import sqlite3
import os
import time
from tqdm import tqdm

def corrigir_em_lotes_estavel(db_path, tamanho_lote=1000, max_lotes=None):
    """Versão estável para processamento em lotes"""
    print(f"Iniciando correção em lotes no banco {db_path}...")
    
    if not os.path.exists(db_path):
        print(f"Erro: Banco de dados não encontrado: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Configurações moderadas para melhorar desempenho sem comprometer estabilidade
    cursor.execute("PRAGMA synchronous = NORMAL")  # Menos agressivo que OFF
    cursor.execute("PRAGMA journal_mode = WAL")    # Write-Ahead Log é mais seguro que MEMORY
    cursor.execute("PRAGMA cache_size = 10000")    # Cache menor, mais estável
    
    try:
        # Verificar total de registros a processar
        cursor.execute("""
        SELECT COUNT(*) FROM socios 
        WHERE cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
            OR LENGTH(cpf_miolo) != 6
            OR cpf_miolo IS NULL 
            OR cpf_miolo = ''
        """)
        total_registros = cursor.fetchone()[0]
        print(f"Total de {total_registros} registros para processar")
        
        # CORREÇÃO: Verificar se tabela de controle existe e tem estrutura correta
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='correcao_controle'")
        tabela_existe = cursor.fetchone() is not None
        
        if tabela_existe:
            # Verificar estrutura da tabela
            cursor.execute("PRAGMA table_info(correcao_controle)")
            colunas = [col[1] for col in cursor.fetchall()]
            
            # Se a estrutura não for a esperada, recriar a tabela
            if 'ultimo_id' not in colunas or 'registros_processados' not in colunas:
                cursor.execute("DROP TABLE correcao_controle")
                tabela_existe = False
        
        # Criar tabela se não existir
        if not tabela_existe:
            cursor.execute("""
            CREATE TABLE correcao_controle (
                ultimo_id INTEGER, 
                registros_processados INTEGER
            )
            """)
            cursor.execute("INSERT INTO correcao_controle VALUES (0, 0)")
            conn.commit()
        
        # Obter progresso
        cursor.execute("SELECT ultimo_id, registros_processados FROM correcao_controle ORDER BY ROWID DESC LIMIT 1")
        
        ultimo_id = 0
        processados = 0
        
        result = cursor.fetchone()
        if result:
            ultimo_id, processados = result
            print(f"Retomando de progresso anterior: ID {ultimo_id}, {processados} registros processados")
        
        inicio_geral = time.time()
        lotes_processados = 0
        
        with tqdm(total=total_registros, initial=processados, desc="Processando", unit="reg") as pbar:
            while True:
                # Buscar próximo lote
                cursor.execute(f"""
                SELECT rowid, [***331355**]
                FROM socios 
                WHERE rowid > ? AND (
                    cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
                    OR LENGTH(cpf_miolo) != 6
                    OR cpf_miolo IS NULL 
                    OR cpf_miolo = ''
                )
                ORDER BY rowid
                LIMIT ?
                """, (ultimo_id, tamanho_lote))
                
                lote = cursor.fetchall()
                
                if not lote:
                    print("\nTodos os registros processados. Concluído!")
                    break
                
                # Processar lote
                for rowid, cpf_original in lote:
                    miolo = ""
                    if cpf_original:
                        # Remover caracteres não numéricos
                        if isinstance(cpf_original, str):
                            digitos = ''.join(c for c in cpf_original if c.isdigit())
                            
                            # Extrair miolo conforme formato
                            if '***' in cpf_original:
                                # Formato: ***XXXXXX**
                                if len(digitos) >= 6:
                                    miolo = digitos[:6]
                            elif len(digitos) >= 11:
                                # CPF completo
                                miolo = digitos[3:9]
                            elif len(digitos) >= 6:
                                # Parcial mas com digitos suficientes
                                miolo = digitos[:6]
                    
                    # Atualizar registro - um de cada vez para evitar bloqueios
                    cursor.execute("""
                    UPDATE socios
                    SET cpf_miolo = ?
                    WHERE rowid = ?
                    """, (miolo, rowid))
                    
                    # Atualizar último ID processado
                    ultimo_id = max(ultimo_id, rowid)
                
                # Commit após cada lote
                conn.commit()
                
                # Atualizar contadores
                processados += len(lote)
                lotes_processados += 1
                pbar.update(len(lote))
                
                # Atualizar controle
                cursor.execute("DELETE FROM correcao_controle")
                cursor.execute("INSERT INTO correcao_controle VALUES (?, ?)", (ultimo_id, processados))
                conn.commit()
                
                # Mostrar estatísticas
                if lotes_processados % 5 == 0:
                    tempo_total = time.time() - inicio_geral
                    velocidade = processados / tempo_total
                    
                    # Estimar tempo restante
                    if velocidade > 0:
                        segundos = (total_registros - processados) / velocidade
                        tempo_restante = f"{int(segundos//3600)}h {int((segundos%3600)//60)}m {int(segundos%60)}s"
                        
                        print(f"\nProgresso: {processados}/{total_registros} ({processados/total_registros*100:.1f}%)")
                        print(f"Velocidade: {velocidade:.1f} reg/s")
                        print(f"Tempo restante: {tempo_restante}")
                
                # Verificar limite de lotes
                if max_lotes and lotes_processados >= max_lotes:
                    print(f"\nLimite de {max_lotes} lotes atingido. Parando.")
                    break
                
                # Permitir interrupção controlada
                if lotes_processados % 10 == 0 and lotes_processados > 0:
                    try:
                        resposta = input("\nContinuar processamento? (S/n): ")
                        if resposta.lower() == 'n':
                            print("Processamento pausado pelo usuário.")
                            break
                    except:
                        pass  # Ignora erros de input em ambientes não interativos
                
                # Liberação periódica de memória
                if lotes_processados % 20 == 0:
                    import gc
                    gc.collect()
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
    finally:
        # Restaurar configurações padrão
        cursor.execute("PRAGMA synchronous = FULL")
        cursor.execute("PRAGMA journal_mode = DELETE")
        conn.commit()
        conn.close()
        print("\nProcessamento finalizado!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Corrige a coluna cpf_miolo em lotes (versão estável)')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--lote', type=int, default=1000, help='Tamanho do lote')
    parser.add_argument('--max', type=int, default=None, help='Número máximo de lotes a processar')
    
    args = parser.parse_args()
    
    corrigir_em_lotes_estavel(args.banco, args.lote, args.max)