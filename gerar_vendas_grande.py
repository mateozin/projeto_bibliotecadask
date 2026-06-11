"""
arquivo para gerar dados para o teste pandas x dask com mais ou menos 320MB de arquivos 
aproximadamente 12.000.000 de registros e apenas 3 colunas
"""

import pandas as pd
import numpy as np
import os
import time
import calendar

def gerar_dados_gr():
    SEED       = 42
    LINHAS_MES = 2_000_000        # 2M por mês -> 12M no total
    PASTA      = "dados_grande"
    CATEGORIAS = ["Eletrônicos", "Vestuário", "Alimentos", "Móveis", "Esportes", "Beleza"]
    MESES      = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]

    PRECOS = {
        "Eletrônicos": (199.90, 4999.00),
        "Vestuário":   ( 29.90,  499.90),
        "Alimentos":   (  5.00,  150.00),
        "Móveis":      (299.00, 3500.00),
        "Esportes":    ( 39.90,  899.00),
        "Beleza":      ( 15.00,  299.00),
    }

    rng = np.random.default_rng(SEED)
    os.makedirs(PASTA, exist_ok=True)

    total_linhas = LINHAS_MES * len(MESES)
    print(f"\n{'='*55}")
    print(f"  Gerando dataset: GRANDE")
    print(f"  ~{total_linhas:,} linhas  |  ~325 MB  |  pasta: {PASTA}/")
    print(f"{'='*55}")

    total_inicio = time.time()

    for mes in MESES:
        inicio      = time.time()
        ano, num_mes = map(int, mes.split("-"))
        dias_no_mes  = calendar.monthrange(ano, num_mes)[1]
        dias  = rng.integers(1, dias_no_mes + 1, size=LINHAS_MES)
        datas = [f"{mes}-{d:02d}" for d in dias]

        pesos  = [0.30, 0.20, 0.20, 0.10, 0.12, 0.08]
        cats   = rng.choice(CATEGORIAS, size=LINHAS_MES, p=pesos)
        valores = np.array([round(rng.uniform(*PRECOS[c]), 2) for c in cats])

        nulos = rng.random(LINHAS_MES) < 0.03
        valores_com_nulo = valores.astype(object)
        valores_com_nulo[nulos] = None

        df   = pd.DataFrame({"data": datas, "categoria": cats, "valor": valores_com_nulo})
        nome = os.path.join(PASTA, f"vendas_{mes.replace('-', '_')}.csv")
        df.to_csv(nome, index=False)

        mb      = os.path.getsize(nome) / (1024 * 1024)
        elapsed = time.time() - inicio
        print(f"  {nome}  ({len(df):,} linhas, {mb:.1f} MB)  [{elapsed:.1f}s]")

    total    = time.time() - total_inicio
    total_mb = sum(os.path.getsize(os.path.join(PASTA, f)) for f in os.listdir(PASTA)) / (1024 * 1024)
    print(f"\n  Total : {total_linhas:,} linhas  |  {total_mb:.1f} MB  |  {total:.1f}s\n")

if __name__ == "__main__":
    gerar_dados_gr()
