#!/usr/bin/env python3
# download_base_completa.py - Script para baixar a base completa da Receita Federal
import os
import requests
import sys
from tqdm import tqdm
import concurrent.futures
import argparse

def download_file(url, output_path, attempt=1, max_attempts=3):
    """Baixa um arquivo da URL para o caminho especificado com barra de progresso"""
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"Arquivo já existe: {output_path} ({file_size/1024/1024:.1f} MB)")
        return True
    
    try:
        # Cria o diretório de saída se não existir
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Faz a requisição com streaming
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Obtém o tamanho total do arquivo
        total_size = int(response.headers.get('content-length', 0))
        
        # Baixa o arquivo com barra de progresso
        with open(output_path, 'wb') as f, tqdm(
                desc=os.path.basename(output_path),
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                bar.update(size)
        
        return True
        
    except requests.exceptions.RequestException as e:
        if attempt < max_attempts:
            print(f"Erro ao baixar {url}: {e}. Tentativa {attempt} de {max_attempts}")
            return download_file(url, output_path, attempt + 1, max_attempts)
        else:
            print(f"Erro ao baixar {url} após {max_attempts} tentativas: {e}")
            return False
    except Exception as e:
        print(f"Erro inesperado ao baixar {url}: {e}")
        return False

def baixar_base_completa(output_dir, num_workers=3, socios_only=False):
    """
    Baixa a base completa da Receita Federal
    
    Args:
        output_dir (str): Diretório de saída
        num_workers (int): Número de workers para download paralelo
        socios_only (bool): Se True, baixa apenas os arquivos de sócios
    """
    # URL base da Receita Federal
    base_url = "https://dadosabertos.rfb.gov.br/CNPJ/"
    
    # Criar diretório de saída
    os.makedirs(output_dir, exist_ok=True)
    
    # Construir URLs para os arquivos
    arquivos = []
    
    # Arquivos de empresas (10 arquivos)
    if not socios_only:
        for i in range(10):
            arquivos.append({
                'url': f"{base_url}/Empresas{i}.zip",
                'output': os.path.join(output_dir, f"empresas{i}.zip")
            })
    
    # Arquivos de estabelecimentos (10 arquivos)
    if not socios_only:
        for i in range(10):
            arquivos.append({
                'url': f"{base_url}/Estabelecimentos{i}.zip",
                'output': os.path.join(output_dir, f"estabelecimentos{i}.zip")
            })
    
    # Arquivos de sócios (9 arquivos)
    for i in range(9):
        arquivos.append({
            'url': f"{base_url}/Socios{i}.zip",
            'output': os.path.join(output_dir, f"socios{i}.zip")
        })
    
    # Arquivos de tabelas auxiliares
    arquivos_aux = [
        {'url': f"{base_url}/Cnaes.zip", 'output': os.path.join(output_dir, "cnaes.zip")},
        {'url': f"{base_url}/Motivos.zip", 'output': os.path.join(output_dir, "motivos.zip")},
        {'url': f"{base_url}/Municipios.zip", 'output': os.path.join(output_dir, "municipios.zip")},
        {'url': f"{base_url}/Naturezas.zip", 'output': os.path.join(output_dir, "naturezas.zip")},
        {'url': f"{base_url}/Paises.zip", 'output': os.path.join(output_dir, "paises.zip")},
        {'url': f"{base_url}/Qualificacoes.zip", 'output': os.path.join(output_dir, "qualificacoes.zip")},
    ]
    
    if not socios_only:
        arquivos.extend(arquivos_aux)
    
    # Download paralelo
    print(f"Iniciando download de {len(arquivos)} arquivos com {num_workers} workers")
    sucessos = 0
    falhas = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {}
        
        for arquivo in arquivos:
            future = executor.submit(download_file, arquivo['url'], arquivo['output'])
            futures[future] = arquivo
        
        for future in concurrent.futures.as_completed(futures):
            arquivo = futures[future]
            try:
                if future.result():
                    sucessos += 1
                else:
                    falhas += 1
            except Exception as e:
                print(f"Erro ao baixar {arquivo['url']}: {e}")
                falhas += 1
    
    print(f"Download concluído: {sucessos} sucessos, {falhas} falhas")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download da base completa da Receita Federal")
    parser.add_argument("--dir", type=str, default="base_completa", help="Diretório de saída")
    parser.add_argument("--workers", type=int, default=3, help="Número de workers para download paralelo")
    parser.add_argument("--socios-only", action="store_true", help="Baixar apenas os arquivos de sócios")
    
    args = parser.parse_args()
    
    print(f"Iniciando download da base {'completa' if not args.socios_only else 'de sócios'}")
    baixar_base_completa(args.dir, args.workers, args.socios_only)