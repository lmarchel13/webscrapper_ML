import pandas as pd
from pandas.io import gbq

def commitData(filename):
    """
        FUNÇÃO QUE LÊ O DATASET JÁ AVALIADO E LIMPO E INSERE
        ATRAVÉS DA LIB PANDAS NO DATABASE DO GOOGLE BIGQUERY
        FORNECIDO ATRAVÉS DAS VARIAVEIS ABAIXO
    """
    dataset = 'dataset_marchel'
    table = 'data_table'
    projectID = 'data-team-test-777'
    destinationTable = dataset+'.'+table

    data = pd.read_csv(filename)
    data.to_gbq(
        destination_table=destinationTable,
        project_id=projectID,
        if_exists='replace'
    )

    print('[Dados enviados ao Google BigQuery com sucesso]')
