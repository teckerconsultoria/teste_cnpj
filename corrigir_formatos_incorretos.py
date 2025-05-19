#!/usr/bin/env python3
# corrigir_formatos_incorretos.py - Foca apenas nos registros com formato incorreto
import sqlite3
import os
import time
from tqdm import tqdm

def corrigir_formatos_incorretos(db_path, tamanho_lote=1000):
    """Corrige apenas os registros com formato incorreto"""
    print(f"Iniciando correção de formatos incorretos em {db_path}...")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Configurar para melhor desempenho
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA journal_mode = WAL")
    
    try:
        # Contar registros a corrigir
        cursor.execute("""
        SELECT COUNT(*) FROM socios
        WHERE cpf_miolo IS NOT NULL AND cpf_miolo != '' 
        AND (LENGTH(cpf_miolo) != 6 OR cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]')
        """)
        
        total_incorretos = cursor.fetchone()[0]
        print(f"Encontrados {total_incorretos:,} registros com formato incorreto")
        
        if total_incorretos == 0:
            print("Nenhum registro para corrigir!")
            return
        
        # Processar em lotes
        processados = 0
        inicio = time.time()
        
        with tqdm(total=total_incorretos, desc="Corrigindo", unit="reg") as pbar:
            while True:
                # Buscar um lote de registros incorretos
                cursor.execute(f"""
                SELECT rowid, [***331355**]
                FROM socios
                WHERE cpf_miolo IS NOT NULL AND cpf_miolo != '' 
                AND (LENGTH(cpf_miolo) != 6 OR cpf_miolo NOT GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]')
                LIMIT {tamanho_lote}
                """)
                
                lote = cursor.fetchall()
                if not lote:
                    break
                
                # Processar lote
                cursor.execute("BEGIN TRANSACTION")
                for rowid, cpf_original in lote:
                    miolo = ""
                    if cpf_original:
                        # Processamento mais agressivo para extrair exatamente 6 dígitos
                        if isinstance(cpf_original, str):
                            # Extrair todos os dígitos
                            digitos = ''.join(c for c in cpf_original if c.isdigit())
                            
                            # Aplicar regras específicas para formato
                            if '***' in cpf_original:
                                # Formato: ***XXXXXX**
                                if len(digitos) >= 6:
                                    miolo = digitos[:6].zfill(6)  # Garantir 6 dígitos
                            elif len(digitos) >= 11:
                                # CPF completo
                                miolo = digitos[3:9]
                            elif len(digitos) >= 6:
                                # Usar primeiros 6 dígitos
                                miolo = digitos[:6].zfill(6)
                            else:
                                # Preencher com zeros se necessário
                                miolo = digitos.zfill(6)
                    
                    # Validar formato final
                    if len(miolo) != 6 or not miolo.isdigit():
                        miolo = '000000'  # Valor padrão para casos irrecuperáveis
                    
                    # Atualizar registro
                    cursor.execute("""
                    UPDATE socios
                    SET cpf_miolo = ?
                    WHERE rowid = ?
                    """, (miolo, rowid))
                
                cursor.execute("COMMIT")
                
                # Atualizar progresso
                processados += len(lote)
                pbar.update(len(lote))
                
                # Mostrar estatísticas periódicas
                if processados % 10000 == 0:
                    tempo_decorrido = time.time() - inicio
                    velocidade = processados / tempo_decorrido if tempo_decorrido > 0 else 0
                    print(f"\nProgresso: {processados:,}/{total_incorretos:,} ({processados/total_incorretos*100:.1f}%)")
                    print(f"Velocidade: {velocidade:.1f} registros/segundo")
        
        tempo_total = time.time() - inicio
        print(f"\nCorreção concluída! {processados:,} registros corrigidos em {tempo_total:.1f} segundos")
        print(f"Velocidade média: {processados/tempo_total:.1f} registros/segundo")
    
    except Exception as e:
        print(f"Erro durante a correção: {e}")
    
    finally:
        # Restaurar configurações
        cursor.execute("PRAGMA synchronous = FULL")
        cursor.execute("PRAGMA journal_mode = DELETE")
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Corrige registros com formato incorreto')
    parser.add_argument('--banco', type=str, default='cnpj_amostra.db', help='Caminho para o banco de dados')
    parser.add_argument('--lote', type=int, default=5000, help='Tamanho do lote')
    
    args = parser.parse_args()
    
    corrigir_formatos_incorretos(args.banco, args.lote)