"""
arquivo para gerar dados para o teste pandas x dask com mais ou menos 320MB de arquivos 
aproximadamente 3.600.000 de registros com 15 colunas e dados nulos, duplicatas ou errados
-> esse cenário mostra onde o dask pode ter desvantagens:
múltiplas agregações = multiplas leituras de disco
"""

import pandas as pd
import numpy as np
import os
import time
import calendar

def gerar_dados_grcomp():
    SEED       = 42
    LINHAS_MES = 300_000          # 300k por mês -> 3.6M no total (12 meses)
    PASTA      = "dados_grandecomp"
    CATEGORIAS = ["Eletrônicos", "Vestuário", "Alimentos", "Móveis",
                  "Esportes", "Beleza", "Brinquedos", "Automotivo"]
    MESES      = [f"2024-{m:02d}" for m in range(1, 13)]  # 12 meses
    REGIOES    = ["Sudeste", "Sul", "Nordeste", "Norte", "Centro-Oeste"]
    CANAIS     = ["E-commerce", "Loja Física", "App Mobile", "Televendas", "Marketplace"]
    VENDEDORES = [f"V{i:04d}" for i in range(1, 201)]

    PRECOS = {
        "Eletrônicos": (199.90, 8999.00),
        "Vestuário":   ( 29.90,  799.90),
        "Alimentos":   (  5.00,  250.00),
        "Móveis":      (299.00, 6500.00),
        "Esportes":    ( 39.90, 1499.00),
        "Beleza":      ( 15.00,  599.00),
        "Brinquedos":  ( 19.90,  899.00),
        "Automotivo":  ( 49.90, 2999.00),
    }

    rng = np.random.default_rng(SEED)
    os.makedirs(PASTA, exist_ok=True)

    total_linhas = LINHAS_MES * len(MESES)
    print(f"\n{'='*55}")
    print(f"  Gerando dataset: GRANDE COMPLEXO")
    print(f"  ~{total_linhas:,} linhas  |  ~321 MB  |  pasta: {PASTA}/")
    print(f"  15 colunas + dados sujos (nulos, negativos)")
    print(f"{'='*55}")

    total_inicio = time.time()

    for mes in MESES:
        inicio      = time.time()
        ano, num_mes = map(int, mes.split("-"))
        dias_no_mes  = calendar.monthrange(ano, num_mes)[1]
        dias  = rng.integers(1, dias_no_mes + 1, size=LINHAS_MES)

        pesos_cat = [0.25, 0.18, 0.18, 0.08, 0.10, 0.08, 0.07, 0.06]
        cats      = rng.choice(CATEGORIAS, size=LINHAS_MES, p=pesos_cat)
        vals      = np.array([round(rng.uniform(*PRECOS[c]), 2) for c in cats])
        qtd       = rng.integers(1, 6, size=LINHAS_MES)
        desc      = rng.choice([0, 5, 10, 15, 20, 30], size=LINHAS_MES,
                               p=[0.40, 0.20, 0.15, 0.12, 0.08, 0.05]).astype(float)

        df = pd.DataFrame({
            "data":         [f"{mes}-{d:02d}" for d in dias],
            "categoria":    cats,
            "subcategoria": rng.choice(["A", "B", "C", "D"], size=LINHAS_MES),
            "valor_unit":   vals,
            "quantidade":   qtd,
            "desconto_pct": desc,
            "valor_total":  (vals * qtd * (1 - desc / 100)).round(2),
            "parcelas":     rng.choice([1, 2, 3, 6, 10, 12], size=LINHAS_MES),
            "regiao":       rng.choice(REGIOES, size=LINHAS_MES),
            "canal":        rng.choice(CANAIS,  size=LINHAS_MES),
            "vendedor_id":  rng.choice(VENDEDORES, size=LINHAS_MES),
            "avaliacao":    rng.choice([1., 2., 3., 4., 5.], size=LINHAS_MES,
                                       p=[0.05, 0.08, 0.15, 0.32, 0.40]),
            "frete":        np.round(rng.uniform(0, 80, size=LINHAS_MES), 2),
            "devolvido":    rng.choice([True, False], size=LINHAS_MES, p=[0.04, 0.96]),
            "cliente_id":   rng.integers(1, 50001, size=LINHAS_MES),
        })

        # 3% nulos em colunas críticas
        for col in ["valor_total", "categoria", "regiao"]:
            mask = rng.random(LINHAS_MES) < 0.03
            df.loc[mask, col] = None

        # 0.5% valores negativos inválidos
        mask_neg = rng.random(len(df)) < 0.005
        df.loc[mask_neg, "valor_total"] = -abs(df.loc[mask_neg, "valor_total"])

        nome = os.path.join(PASTA, f"vendas_{mes.replace('-', '_')}.csv")
        df.to_csv(nome, index=False)

        mb      = os.path.getsize(nome) / (1024 * 1024)
        elapsed = time.time() - inicio
        print(f"  {nome}  ({len(df):,} linhas, {mb:.1f} MB)  [{elapsed:.1f}s]")

    total    = time.time() - total_inicio
    total_mb = sum(os.path.getsize(os.path.join(PASTA, f)) for f in os.listdir(PASTA)) / (1024 * 1024)
    print(f"\n  Total : {total_linhas:,} linhas  |  {total_mb:.1f} MB  |  {total:.1f}s\n")

if __name__ == "__main__":
    gerar_dados_grcomp()
