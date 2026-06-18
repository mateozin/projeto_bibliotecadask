"""
le os dados (.csv) criados no código anterior usando pandas ou dask
processa os dados em Pandas e em Dask e compara o desempenho gerando estatisticas
e gráficos
"""

import time
import glob
import os
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from gerar_vendas_medio import gerar_dados_med
from gerar_vendas_pequeno import gerar_dados_peq
from gerar_vendas_grande import gerar_dados_gr
from gerar_vendas_grande_complexo import gerar_dados_grcomp

if len(glob.glob("dados_grandecomp/*.csv")) == 0:
    nogrcomp = 1
    yn = input("Você deseja criar os [GRANDE COMPLEXO] .csv (~30s)? (Y/N)")
    if yn == "y" or yn == "Y":
        print("Nenhum [GRANDE COMPLEXO] .csv encontrado, criando novos...")
        gerar_dados_grcomp()
    else:
        nogrcomp = 0
else:
    nogrcomp = 1
    print("Arquivos .csv já existente, executando o código normalmente...")
if len(glob.glob("dados_grande/*.csv")) == 0:
    nogr = 1
    yn = input("Você deseja criar os [GRANDE] .csv (~50s)? (Y/N)")
    if yn == "y" or yn == "Y":
        print("Nenhum [GRANDE] .csv encontrado, criando novos...")
        gerar_dados_gr()
    else:
        nogr = 0
else:
    nogr = 1
    print("Arquivos .csv já existente, executando o código normalmente...")
if len(glob.glob("dados_medio/*.csv")) == 0:
    print("Nenhum [MÉDIO] .csv encontrado, criando novos..")
    gerar_dados_med()
else:
    print("Arquivos .csv já existente, executando o código normalmente...")
if len(glob.glob("dados_pequeno/*.csv")) == 0:
    print("Nenhum [PEQUENO] .csv encontrado, criando novos.. [pequeno]")
    gerar_dados_peq()
else:
    print("Arquivos .csv já existente, executando o código normalmente... [pequeno]")

escolha = input('Digite uma opção: \n1- Pequeno (1Mb) \n2 - Médio (80Mb) \n3- Grande (330Mb) \n4 - Grande e Complexo (320Mb mas 15 colunas)\n>>')

#PEQUENO
if escolha == "1":
    PASTA = "dados_pequeno/vendas_*.csv"

    # PANDAS
    print("=" * 55)
    print("  PANDAS — carrega tudo na memória de uma vez")
    print("=" * 55)

    inicio_pandas = time.time()

    print("1. Lendo todos os CSVs para a memória...")
    t = time.time()
    arquivos = glob.glob(PASTA)
    df_pd = pd.concat(
        [pd.read_csv(f, parse_dates=["data"]) for f in sorted(arquivos)],
        ignore_index=True
    )
    print(f"   {len(df_pd):,} linhas  ({time.time()-t:.2f}s)")

    print("2. Limpando dados...")
    df_pd = df_pd.dropna(subset=["valor", "categoria"])
    df_pd["valor"] = df_pd["valor"].astype("float32")

    print("3. Criando coluna 'mes'...")
    df_pd["mes"] = df_pd["data"].dt.to_period("M").astype("str")

    print("4. groupby + sum...")
    resultado_pd = (
        df_pd
        .groupby(["categoria", "mes"])["valor"]
        .sum()
        .reset_index()
        .sort_values(["mes", "valor"], ascending=[True, False])
    )

    tempo_pandas = time.time() - inicio_pandas
    print(f"\nPandas: {tempo_pandas:.2f}s")

    # DASK
    print()
    print("=" * 55)
    print("  DASK — processamento paralelo, lazy evaluation")
    print("=" * 55)

    inicio_dask = time.time()

    print("1. Leitura lazy (sem carregar na memória)...")
    df_dk = dd.read_csv(PASTA, parse_dates=["data"])
    print(f"   Partições: {df_dk.npartitions}")

    print("2. Limpando dados (lazy)...")
    df_dk = df_dk.dropna(subset=["valor", "categoria"])
    df_dk["valor"] = df_dk["valor"].astype("float32")

    print("3. Criando coluna 'mes' (lazy)...")
    df_dk["mes"] = df_dk["data"].dt.to_period("M").astype("str")

    print("4. groupby + sum + .compute()...")
    resultado_dk = (
        df_dk
        .groupby(["categoria", "mes"])["valor"]
        .sum()
        .reset_index()
        .compute()
        .sort_values(["mes", "valor"], ascending=[True, False])
    )

    tempo_dask = time.time() - inicio_dask
    print(f"\nDask: {tempo_dask:.2f}s")

    # RELATÓRIO FINAL
    print()
    print("=" * 55)
    print("  RESULTADO FINAL - BANCO DE DADOS PEQUENO")
    print("=" * 55)

    tamanho_mb = sum(
        os.path.getsize(f) for f in glob.glob(PASTA)
    ) / (1024 * 1024)

    total_linhas = len(df_pd)

    print(f"  Dados          : {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)")
    print(f"  Pandas         : {tempo_pandas:.2f}s")
    print(f"  Dask           : {tempo_dask:.2f}s")

    if tempo_pandas > tempo_dask:
        fator = tempo_pandas / tempo_dask
        print(f"-> Dask foi {fator:.1f}x mais rápido")
    else:
        fator = tempo_dask / tempo_pandas
        print(f"-> Pandas foi {fator:.1f}x mais rápido (volume ainda pequeno)")

    print(f"  Resultados iguais? {resultado_pd.shape == resultado_dk.shape}")
    print("=" * 55)

    # GRÁFICOS
    fig = plt.figure(figsize=(14, 9))
    fig.suptitle(
        f"Pandas x Dask  —  {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)",
        fontsize=15, fontweight="bold"
    )

    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

    # barras de tempo
    ax1 = fig.add_subplot(gs[0, 0])
    libs   = ["Pandas", "Dask"]
    tempos = [tempo_pandas, tempo_dask]
    cores  = ["#F59E0B", "#2563EB"]
    barras = ax1.bar(libs, tempos, color=cores, width=0.45, zorder=3)
    for bar, t in zip(barras, tempos):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.03,
                f"{t:.2f}s", ha="center", va="bottom",
                fontsize=12, fontweight="bold")
    ax1.set_title("Tempo de execução", fontweight="bold")
    ax1.set_ylabel("Segundos")
    ax1.set_ylim(0, max(tempos) * 1.35)
    ax1.spines[["top", "right"]].set_visible(False)
    ax1.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax1.set_axisbelow(True)

    # fator de diferença
    ax2 = fig.add_subplot(gs[0, 1])
    vencedor = "Dask" if tempo_dask < tempo_pandas else "Pandas"
    cor_venc = "#2563EB" if vencedor == "Dask" else "#F59E0B"
    ax2.text(0.5, 0.58, f"{fator:.1f}x",
            ha="center", va="center", fontsize=52,
            fontweight="bold", color=cor_venc,
            transform=ax2.transAxes)
    ax2.text(0.5, 0.28, f"{vencedor} mais rápido",
            ha="center", va="center", fontsize=13,
            color="#475569", transform=ax2.transAxes)
    ax2.set_title("Fator de velocidade", fontweight="bold")
    ax2.axis("off")

    #faturamento por categoria | Dask = Pandas
    ax3 = fig.add_subplot(gs[1, :])
    resumo_cat = resultado_dk.groupby("categoria")["valor"].sum().sort_values(ascending=True)
    cores3 = ["#2563EB", "#0D9488", "#10B981", "#7C3AED", "#F59E0B", "#EF4444"]
    b3 = ax3.barh(resumo_cat.index, resumo_cat.values,
                color=cores3[:len(resumo_cat)], zorder=3)
    ax3.bar_label(b3, labels=[f"R$ {v/1e6:.1f} M" for v in resumo_cat.values],
                padding=4, fontsize=9)
    ax3.set_title("Faturamento por categoria", fontweight="bold")
    ax3.set_xlabel("R$")
    ax3.set_xlim(0, resumo_cat.max() * 1.22)
    ax3.spines[["top", "right"]].set_visible(False)
    ax3.xaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax3.set_axisbelow(True)

    os.makedirs("output", exist_ok=True)
    plt.savefig("output/comparacao_peq.png", dpi=150, bbox_inches="tight")
    print("Gráfico salvo em output/comparacao_peq.png")

elif escolha == "2":
    PASTA = "dados_medio/vendas_*.csv"

    # PANDAS
    print("=" * 55)
    print("  PANDAS — carrega tudo na memória de uma vez")
    print("=" * 55)

    inicio_pandas = time.time()

    print("1. Lendo todos os CSVs para a memória...")
    t = time.time()
    arquivos = glob.glob(PASTA)
    df_pd = pd.concat(
        [pd.read_csv(f, parse_dates=["data"]) for f in sorted(arquivos)],
        ignore_index=True
    )
    print(f"   {len(df_pd):,} linhas  ({time.time()-t:.2f}s)")

    print("2. Limpando dados...")
    df_pd = df_pd.dropna(subset=["valor", "categoria"])
    df_pd["valor"] = df_pd["valor"].astype("float32")

    print("3. Criando coluna 'mes'...")
    df_pd["mes"] = df_pd["data"].dt.to_period("M").astype("str")

    print("4. groupby + sum...")
    resultado_pd = (
        df_pd
        .groupby(["categoria", "mes"])["valor"]
        .sum()
        .reset_index()
        .sort_values(["mes", "valor"], ascending=[True, False])
    )

    tempo_pandas = time.time() - inicio_pandas
    print(f"\nPandas: {tempo_pandas:.2f}s")

    # DASK
    print()
    print("=" * 55)
    print("  DASK — processamento paralelo, lazy evaluation")
    print("=" * 55)

    inicio_dask = time.time()

    print("1. Leitura lazy (sem carregar na memória)...")
    df_dk = dd.read_csv(PASTA, parse_dates=["data"])
    print(f"   Partições: {df_dk.npartitions}")

    print("2. Limpando dados (lazy)...")
    df_dk = df_dk.dropna(subset=["valor", "categoria"])
    df_dk["valor"] = df_dk["valor"].astype("float32")

    print("3. Criando coluna 'mes' (lazy)...")
    df_dk["mes"] = df_dk["data"].dt.to_period("M").astype("str")

    print("4. groupby + sum + .compute()...")
    resultado_dk = (
        df_dk
        .groupby(["categoria", "mes"])["valor"]
        .sum()
        .reset_index()
        .compute()
        .sort_values(["mes", "valor"], ascending=[True, False])
    )

    tempo_dask = time.time() - inicio_dask
    print(f"\nDask: {tempo_dask:.2f}s")

    # RELATÓRIO FINAL
    print()
    print("=" * 55)
    print("  RESULTADO FINAL - BANCO DE DADOS MÉDIO")
    print("=" * 55)

    tamanho_mb = sum(
        os.path.getsize(f) for f in glob.glob(PASTA)
    ) / (1024 * 1024)

    total_linhas = len(df_pd)

    print(f"  Dados          : {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)")
    print(f"  Pandas         : {tempo_pandas:.2f}s")
    print(f"  Dask           : {tempo_dask:.2f}s")

    if tempo_pandas > tempo_dask:
        fator = tempo_pandas / tempo_dask
        print(f"-> Dask foi {fator:.1f}x mais rápido")
    else:
        fator = tempo_dask / tempo_pandas
        print(f"-> Pandas foi {fator:.1f}x mais rápido (volume ainda pequeno)")

    print(f"  Resultados iguais? {resultado_pd.shape == resultado_dk.shape}")
    print("=" * 55)

    # GRÁFICOS
    fig = plt.figure(figsize=(14, 9))
    fig.suptitle(
        f"Pandas x Dask  —  {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)",
        fontsize=15, fontweight="bold"
    )

    gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

    # barras de tempo
    ax1 = fig.add_subplot(gs[0, 0])
    libs   = ["Pandas", "Dask"]
    tempos = [tempo_pandas, tempo_dask]
    cores  = ["#F59E0B", "#2563EB"]
    barras = ax1.bar(libs, tempos, color=cores, width=0.45, zorder=3)
    for bar, t in zip(barras, tempos):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.03,
                f"{t:.2f}s", ha="center", va="bottom",
                fontsize=12, fontweight="bold")
    ax1.set_title("Tempo de execução", fontweight="bold")
    ax1.set_ylabel("Segundos")
    ax1.set_ylim(0, max(tempos) * 1.35)
    ax1.spines[["top", "right"]].set_visible(False)
    ax1.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax1.set_axisbelow(True)

    # fator de diferença
    ax2 = fig.add_subplot(gs[0, 1])
    vencedor = "Dask" if tempo_dask < tempo_pandas else "Pandas"
    cor_venc = "#2563EB" if vencedor == "Dask" else "#F59E0B"
    ax2.text(0.5, 0.58, f"{fator:.1f}x",
            ha="center", va="center", fontsize=52,
            fontweight="bold", color=cor_venc,
            transform=ax2.transAxes)
    ax2.text(0.5, 0.28, f"{vencedor} mais rápido",
            ha="center", va="center", fontsize=13,
            color="#475569", transform=ax2.transAxes)
    ax2.set_title("Fator de velocidade", fontweight="bold")
    ax2.axis("off")

    #faturamento por categoria | Dask = Pandas
    ax3 = fig.add_subplot(gs[1, :])
    resumo_cat = resultado_dk.groupby("categoria")["valor"].sum().sort_values(ascending=True)
    cores3 = ["#2563EB", "#0D9488", "#10B981", "#7C3AED", "#F59E0B", "#EF4444"]
    b3 = ax3.barh(resumo_cat.index, resumo_cat.values,
                color=cores3[:len(resumo_cat)], zorder=3)
    ax3.bar_label(b3, labels=[f"R$ {v/1e6:.1f} M" for v in resumo_cat.values],
                padding=4, fontsize=9)
    ax3.set_title("Faturamento por categoria", fontweight="bold")
    ax3.set_xlabel("R$")
    ax3.set_xlim(0, resumo_cat.max() * 1.22)
    ax3.spines[["top", "right"]].set_visible(False)
    ax3.xaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
    ax3.set_axisbelow(True)

    os.makedirs("output", exist_ok=True)
    plt.savefig("output/comparacao_med.png", dpi=150, bbox_inches="tight")
    print("Gráfico salvo em output/comparacao_med.png")

#Grande
elif escolha == "3":
    if nogr != 0:
        PASTA = "dados_grande/vendas_*.csv"

        # PANDAS
        print("=" * 55)
        print("  PANDAS — carrega tudo na memória de uma vez")
        print("=" * 55)

        inicio_pandas = time.time()

        print("1. Lendo todos os CSVs para a memória...")
        t = time.time()
        arquivos = glob.glob(PASTA)
        df_pd = pd.concat(
            [pd.read_csv(f, parse_dates=["data"]) for f in sorted(arquivos)],
            ignore_index=True
        )
        print(f"   {len(df_pd):,} linhas  ({time.time()-t:.2f}s)")

        print("2. Limpando dados...")
        df_pd = df_pd.dropna(subset=["valor", "categoria"])
        df_pd["valor"] = df_pd["valor"].astype("float32")

        print("3. Criando coluna 'mes'...")
        df_pd["mes"] = df_pd["data"].dt.to_period("M").astype("str")

        print("4. groupby + sum...")
        resultado_pd = (
            df_pd
            .groupby(["categoria", "mes"])["valor"]
            .sum()
            .reset_index()
            .sort_values(["mes", "valor"], ascending=[True, False])
        )

        tempo_pandas = time.time() - inicio_pandas
        print(f"\nPandas: {tempo_pandas:.2f}s")

        # DASK
        print()
        print("=" * 55)
        print("  DASK — processamento paralelo, lazy evaluation")
        print("=" * 55)

        inicio_dask = time.time()

        print("1. Leitura lazy (sem carregar na memória)...")
        df_dk = dd.read_csv(PASTA, parse_dates=["data"])
        print(f"   Partições: {df_dk.npartitions}")

        print("2. Limpando dados (lazy)...")
        df_dk = df_dk.dropna(subset=["valor", "categoria"])
        df_dk["valor"] = df_dk["valor"].astype("float32")

        print("3. Criando coluna 'mes' (lazy)...")
        df_dk["mes"] = df_dk["data"].dt.to_period("M").astype("str")

        print("4. groupby + sum + .compute()...")
        resultado_dk = (
            df_dk
            .groupby(["categoria", "mes"])["valor"]
            .sum()
            .reset_index()
            .compute()
            .sort_values(["mes", "valor"], ascending=[True, False])
        )

        tempo_dask = time.time() - inicio_dask
        print(f"\nDask: {tempo_dask:.2f}s")

        # RELATÓRIO FINAL
        print()
        print("=" * 55)
        print("  RESULTADO FINAL - BANCO DE DADOS GRANDE")
        print("=" * 55)

        tamanho_mb = sum(
            os.path.getsize(f) for f in glob.glob(PASTA)
        ) / (1024 * 1024)

        total_linhas = len(df_pd)

        print(f"  Dados          : {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)")
        print(f"  Pandas         : {tempo_pandas:.2f}s")
        print(f"  Dask           : {tempo_dask:.2f}s")

        if tempo_pandas > tempo_dask:
            fator = tempo_pandas / tempo_dask
            print(f"-> Dask foi {fator:.1f}x mais rápido")
        else:
            fator = tempo_dask / tempo_pandas
            print(f"-> Pandas foi {fator:.1f}x mais rápido")

        print(f"  Resultados iguais? {resultado_pd.shape == resultado_dk.shape}")
        print("=" * 55)

        # GRÁFICOS
        fig = plt.figure(figsize=(14, 9))
        fig.suptitle(
            f"Pandas x Dask  —  {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)",
            fontsize=15, fontweight="bold"
        )

        gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

        # barras de tempo
        ax1 = fig.add_subplot(gs[0, 0])
        libs   = ["Pandas", "Dask"]
        tempos = [tempo_pandas, tempo_dask]
        cores  = ["#F59E0B", "#2563EB"]
        barras = ax1.bar(libs, tempos, color=cores, width=0.45, zorder=3)
        for bar, t in zip(barras, tempos):
            ax1.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(tempos) * 0.02,
                    f"{t:.2f}s", ha="center", va="bottom",
                    fontsize=12, fontweight="bold")
        ax1.set_title("Tempo de execução", fontweight="bold")
        ax1.set_ylabel("Segundos")
        ax1.set_ylim(0, max(tempos) * 1.35)
        ax1.spines[["top", "right"]].set_visible(False)
        ax1.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax1.set_axisbelow(True)

        # fator de diferença
        ax2 = fig.add_subplot(gs[0, 1])
        vencedor = "Dask" if tempo_dask < tempo_pandas else "Pandas"
        cor_venc = "#2563EB" if vencedor == "Dask" else "#F59E0B"
        ax2.text(0.5, 0.58, f"{fator:.1f}x",
                ha="center", va="center", fontsize=52,
                fontweight="bold", color=cor_venc,
                transform=ax2.transAxes)
        ax2.text(0.5, 0.28, f"{vencedor} mais rápido",
                ha="center", va="center", fontsize=13,
                color="#475569", transform=ax2.transAxes)
        ax2.set_title("Fator de velocidade", fontweight="bold")
        ax2.axis("off")

        #faturamento por categoria | Dask = Pandas
        ax3 = fig.add_subplot(gs[1, :])
        resumo_cat = resultado_dk.groupby("categoria")["valor"].sum().sort_values(ascending=True)
        cores3 = ["#2563EB", "#0D9488", "#10B981", "#7C3AED", "#F59E0B", "#EF4444"]
        b3 = ax3.barh(resumo_cat.index, resumo_cat.values,
                    color=cores3[:len(resumo_cat)], zorder=3)
        ax3.bar_label(b3, labels=[f"R$ {v/1e6:.1f} M" for v in resumo_cat.values],
                    padding=4, fontsize=9)
        ax3.set_title("Faturamento por categoria", fontweight="bold")
        ax3.set_xlabel("R$")
        ax3.set_xlim(0, resumo_cat.max() * 1.22)
        ax3.spines[["top", "right"]].set_visible(False)
        ax3.xaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax3.set_axisbelow(True)

        os.makedirs("output", exist_ok=True)
        plt.savefig("output/comparacao_grande.png", dpi=150, bbox_inches="tight")
        print("Gráfico salvo em output/comparacao_grande.png")
    else:
        print("Dados não criados.")
#Grande e Complexo
elif escolha == "4":
    if nogrcomp != 0:
        PASTA = "dados_grandecomp/vendas_*.csv"

        # PANDAS
        print("=" * 55)
        print("  PANDAS — carrega tudo na memória de uma vez")
        print("=" * 55)

        inicio_pandas = time.time()

        print("1. Lendo todos os CSVs para a memória...")
        t = time.time()
        arquivos = glob.glob(PASTA)
        df_pd = pd.concat(
            [pd.read_csv(f, parse_dates=["data"]) for f in sorted(arquivos)],
            ignore_index=True
        )
        print(f"   {len(df_pd):,} linhas  ({time.time()-t:.2f}s)")

        print("2. Limpando dados...")
        df_pd = df_pd.dropna(subset=["valor_total", "categoria", "regiao"])
        df_pd = df_pd[df_pd["valor_total"] > 0]
        df_pd["valor_total"] = df_pd["valor_total"].astype("float32")
        df_pd["avaliacao"]   = df_pd["avaliacao"].astype("float32")

        print("3. Criando coluna 'mes'...")
        df_pd["mes"] = df_pd["data"].dt.to_period("M").astype("str")

        print("4. Agregações...")
        r1 = df_pd.groupby(["categoria", "mes"])["valor_total"].sum().reset_index()
        r2 = df_pd.groupby(["canal", "regiao"])["valor_total"].mean().reset_index()
        r3 = df_pd.groupby("categoria")["avaliacao"].mean().reset_index()
        r4 = df_pd.groupby("vendedor_id")["valor_total"].sum().nlargest(10)
        r5 = df_pd.groupby(["regiao", "canal"])["quantidade"].sum().reset_index()
        resultado_pd = r1.sort_values(["mes", "valor_total"], ascending=[True, False])

        tempo_pandas = time.time() - inicio_pandas
        print(f"\nPandas: {tempo_pandas:.2f}s")

        # DASK
        print()
        print("=" * 55)
        print("  DASK — processamento paralelo, lazy evaluation")
        print("=" * 55)

        inicio_dask = time.time()

        print("1. Leitura lazy (sem carregar na memória)...")
        df_dk = dd.read_csv(PASTA, parse_dates=["data"])
        print(f"   Partições: {df_dk.npartitions}")

        print("2. Limpando dados (lazy)...")
        df_dk = df_dk.dropna(subset=["valor_total", "categoria", "regiao"])
        df_dk = df_dk[df_dk["valor_total"] > 0]
        df_dk["valor_total"] = df_dk["valor_total"].astype("float32")
        df_dk["avaliacao"]   = df_dk["avaliacao"].astype("float32")

        print("3. Criando coluna 'mes' (lazy)...")
        df_dk["mes"] = df_dk["data"].dt.to_period("M").astype("str")

        print("4. Agregações + .compute()...")
        d1 = df_dk.groupby(["categoria", "mes"])["valor_total"].sum().reset_index().compute()
        d2 = df_dk.groupby(["canal", "regiao"])["valor_total"].mean().reset_index().compute()
        d3 = df_dk.groupby("categoria")["avaliacao"].mean().reset_index().compute()
        d4 = df_dk.groupby("vendedor_id")["valor_total"].sum().compute().nlargest(10)
        d5 = df_dk.groupby(["regiao", "canal"])["quantidade"].sum().reset_index().compute()
        resultado_dk = d1.sort_values(["mes", "valor_total"], ascending=[True, False])

        tempo_dask = time.time() - inicio_dask
        print(f"\nDask: {tempo_dask:.2f}s")

        # RELATÓRIO FINAL
        print()
        print("=" * 55)
        print("  RESULTADO FINAL - BANCO DE DADOS GRANDE E COMPLEXO")
        print("=" * 55)

        tamanho_mb = sum(
            os.path.getsize(f) for f in glob.glob(PASTA)
        ) / (1024 * 1024)

        total_linhas = len(df_pd)

        print(f"  Dados          : {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)")
        print(f"  Pandas         : {tempo_pandas:.2f}s")
        print(f"  Dask           : {tempo_dask:.2f}s")

        if tempo_pandas > tempo_dask:
            fator = tempo_pandas / tempo_dask
            print(f"-> Dask foi {fator:.1f}x mais rápido")
        else:
            fator = tempo_dask / tempo_pandas
            print(f"-> Pandas foi {fator:.1f}x mais rápido (múltiplos .compute() penalizam o Dask)")

        print(f"  Resultados iguais? {resultado_pd.shape == resultado_dk.shape}")
        print("=" * 55)

        # GRÁFICOS
        fig = plt.figure(figsize=(14, 9))
        fig.suptitle(
            f"Pandas x Dask  —  {total_linhas:,} linhas  ({tamanho_mb:.0f} MB)",
            fontsize=15, fontweight="bold"
        )

        gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35)

        # barras de tempo
        ax1 = fig.add_subplot(gs[0, 0])
        libs   = ["Pandas", "Dask"]
        tempos = [tempo_pandas, tempo_dask]
        cores  = ["#F59E0B", "#2563EB"]
        barras = ax1.bar(libs, tempos, color=cores, width=0.45, zorder=3)
        for bar, t in zip(barras, tempos):
            ax1.text(bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(tempos) * 0.02,
                    f"{t:.2f}s", ha="center", va="bottom",
                    fontsize=12, fontweight="bold")
        ax1.set_title("Tempo de execução", fontweight="bold")
        ax1.set_ylabel("Segundos")
        ax1.set_ylim(0, max(tempos) * 1.35)
        ax1.spines[["top", "right"]].set_visible(False)
        ax1.yaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax1.set_axisbelow(True)

        # fator de diferença
        ax2 = fig.add_subplot(gs[0, 1])
        vencedor = "Dask" if tempo_dask < tempo_pandas else "Pandas"
        cor_venc = "#2563EB" if vencedor == "Dask" else "#F59E0B"
        ax2.text(0.5, 0.58, f"{fator:.1f}x",
                ha="center", va="center", fontsize=52,
                fontweight="bold", color=cor_venc,
                transform=ax2.transAxes)
        ax2.text(0.5, 0.28, f"{vencedor} mais rápido",
                ha="center", va="center", fontsize=13,
                color="#475569", transform=ax2.transAxes)
        ax2.set_title("Fator de velocidade", fontweight="bold")
        ax2.axis("off")

        #faturamento por categoria | Dask = Pandas
        ax3 = fig.add_subplot(gs[1, :])
        resumo_cat = resultado_dk.groupby("categoria")["valor_total"].sum().sort_values(ascending=True)
        cores3 = ["#2563EB", "#0D9488", "#10B981", "#7C3AED", "#F59E0B", "#EF4444", "#EC4899", "#64748B"]
        b3 = ax3.barh(resumo_cat.index, resumo_cat.values,
                    color=cores3[:len(resumo_cat)], zorder=3)
        ax3.bar_label(b3, labels=[f"R$ {v/1e6:.1f} M" for v in resumo_cat.values],
                    padding=4, fontsize=9)
        ax3.set_title("Faturamento total por categoria", fontweight="bold")
        ax3.set_xlabel("R$")
        ax3.set_xlim(0, resumo_cat.max() * 1.22)
        ax3.spines[["top", "right"]].set_visible(False)
        ax3.xaxis.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax3.set_axisbelow(True)

        os.makedirs("output", exist_ok=True)
        plt.savefig("output/comparacao_grcomp.png", dpi=150, bbox_inches="tight")
        print("Gráfico salvo em output/comparacao_grcomp.png")
    else:
        print("Dados não criados.")
else:
    print("Opção inválida.")
