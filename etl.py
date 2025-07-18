import pandas as pd 
import os #consigo usar os comandos do terminar no programa python
import glob
from utils_log import log_decorator
from timer import time_measure_decorator


# 1- Uma função de extract que lê e consolida os json

@log_decorator
@time_measure_decorator
def extrair_dados_e_consolidar (pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json')) #Vai listar todos os meus arquivos que são json
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total



# 2- Uma função que transforma

@log_decorator
@time_measure_decorator
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df


# 3- Uma função que da load em csv ou parquet
    #Essa função é uma procedure porque não tem um return

@log_decorator
@time_measure_decorator
def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    parametro que vai ser ou "csv" ou "parquet" ou "os dois"
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        if formato == 'parquet':
            df.to_parquet("dados.parquet", index=False)

# 4- Função que vai chamar tudo!
@log_decorator
@time_measure_decorator
def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_de_saida: list):
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado, formato_de_saida)

## Para testar os desenvolvimentos: (Assim não fica nenhum "lixo" no codigo principal)

# if __name__ == "__main__":
#     pasta_argumento: str = 'data'
#     data_frame = extrair_dados_e_consolidar(pasta=pasta_argumento)
#     data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
#     formato_de_saida: list = ["csv", "parquet"]
#     carregar_dados(data_frame_calculado, formato_de_saida)