import time, requests, datetime
import pandas as pd
from selenium import webdriver
#from selenium.webdriver.common.by import By
#from selenium.webdriver.common.keys import Keys
#from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from load_data import commitData

class App(object):

    def __init__(self):
        """
        Declaração de variaveis inicias para uso das mesmas durante o processo de ETL
        """
        print('[Iniciando o web-scrapping da pagina do MercadoLivre]')
        print('[Isso pode demorar alguns segundos, dependendo da velocidade da sua conexão]')
        self.data = list()
        self.filename = 'DogHero-Dataset-master.csv'
        self.clean_filename = 'DogHero-Dataset-clean.csv'

        """
            JUNÇÃO DAS URLS NECESSÁRIAS PARA ITERAÇÃO DO LOOP 
            FILTRANDO APENAS POR PRODUTOS NOVOS (_ItemTypeID_N)
            E ALGUNS ESTADOS DISPONIVEIS PARA ESSE PRODUTO (states)
        """

        self.ROOT_URL = 'https://lista.mercadolivre.com.br/'
        self.product = 'console-playstation-4-1tb-slim'
        self.onlyNewProductsURL = '_ItemTypeID_N'
        self.states = [
            'parana','distrito-federal','rio-de-janeiro',
            'rio-grande-do-sul','santa-catarina','mato-grosso-do-sul','minas-gerais',
            'goias','ceara','espirito-santo','pernambuco','rio-grande-do-norte','sao-paulo'
        ]

        # INICIO DA ITERAÇÃO DO LOOP ENTRE TODOS OS ESTADOS

        for self.state in self.states:
            self.stateName = self.state.replace('-',' ').title()
            self.initialURL = self.ROOT_URL + self.state + '/' + self.product +self.onlyNewProductsURL
            self.openConnection()
            try:
                self.webScrapping()
            except Exception as e:
                print('[ERRO WEBSCRAPPING] '+str(e))

        self.saveDataSet()
        
        self.cleanData()
        
        self.insertData()

    def openConnection(self):
        """
            PRIMEIRO REQUEST PARA VERIFICAR A QUANTIDADE DE RESULTADOS
            E QUANTIDADES DE PAGINAS QUE SERÃO NECESSÁRIAS PARA ALTERAR A URL
        """
        print('[Produto: '+self.product.upper()+']')
        print('[Estado: '+self.stateName+']')
        try:
            response = requests.get(self.initialURL)
            time.sleep(5)
            self.soup = BeautifulSoup(response.content,'html.parser')
            time.sleep(5)
            self.totalResults()
        except Exception as e:
            print('[ERRO CONNECTION] '+str(e))

    def totalResults(self):
        """
            OBTENÇÃO DA QUANTIDADE DE RESULTADOS (PRODUTOS) DISPONVEIS PARA
            DETERMINADO ESTADO
        """
        results = self.soup.find('div',class_='quantity-results').text
        self.results = int(results[:results.find('r')])
        print('[Total de Resultados encontrados: '+str(self.results)+']')
        self.numberOfPages()

    def numberOfPages(self):
        """
            CALCULO DA QUANTIDADE DE PAGINAS QUE SERÃO INSERIDAS NO LOOP
            PARA ALTERAR A URL INICIAL
        """
        resultsPerPage = 50
        self.pages = int(self.results/resultsPerPage) + (self.results % resultsPerPage > 0)
        print('[Total de Páginas: ' + str(self.pages) + ']\n')

    def webScrapping(self):
        """
            DEFINIDA A QUANTIDADE DE PAGINAS, ESSA FUNÇÃO É A RESPONSAVEL POR
            REALIZAR O LOOP ENTRE TODAS AS URLS QUE SERÃO GERADAS ABAIXO E
            ARMZENADAS EM UM LIST
        """
        urls = []
        for n in range(self.pages):
            if n == 0:
                url = self.initialURL
            else:
                url = self.initialURL + '_Desde_' + str((n*50)+1)
            urls.append(url)

        # INICIO DEFINITIVO DO WEBSCRAPPING DE DETERMINADO ESTADO
        for url in urls:
            try:
                driver = webdriver.Chrome()
                driver.get(url)
                time.sleep(5)
                source = driver.page_source
                soup = BeautifulSoup(source, 'html.parser')
                time.sleep(5)
                olTag = soup.find('ol',id="searchResults")
                time.sleep(5)
                liTags = olTag.find_all('li')
                time.sleep(5)
                for li in liTags:
                    item = list()

                    productID = li.div['id']
                    title = li.find('span',class_="main-title").text
                    title = title.replace(',',' ').strip()
                    priceFraction = li.find('span',class_="price__fraction").text
                    priceDecimals = li.find('span',class_="price__decimals")
                    if priceDecimals:
                        priceDecimals = priceDecimals.text
                    else:
                        priceDecimals = '00'
                    price = priceFraction + ',' + priceDecimals

                    item.append(productID)
                    item.append(datetime.datetime.now().strftime("%Y-%m-%d"))
                    item.append(title)
                    item.append(price)
                    item.append(self.stateName)
                    print(item)
                    self.data.append(item)
                driver.quit()
            except Exception as e:
                print('[Erro] - '+str(e))

    def saveDataSet(self):
        """
            AO FINAL DO LOOP ENTRE TODOS OS ESTADOS, É ARMAZENADO ESSE DATASET
            EM UM .CSV PARA POSTERIORMENTE TRATAR OS DADOS OBTIDOS
        """
        print('[Salvando o dataset em um arquivo .csv]')

        df = pd.DataFrame(self.data)
        header = ['ID','Timestamp','Titulo','Preco', 'Estado']
        df.to_csv(self.filename, index=False, header=header)
        print('[Dados salvos com sucesso]')

    def cleanData(self):
        """
            LIMPEZA DO DATASET OBTIDO ATRÁVES DO WEB SCRAPPING
            - Remoção das rows duplicadsa
            - Alteração no datetype da coluuna "Preco" para float
            - Verificação da presença de valores nulos
            - Remoção de valores extremos e discrepantes que influenciam a análise

            FEITO AS LIMPEZAS, É SALVO UM NOVO DATASET EM CSV PARA ENVIA-LO
            AO GOOGLE BIGQUERY
        """
        #Clonagem inicial do dataframe apenas por segurança
        df_clean = pd.read_csv(self.filename)
        df = df_clean.copy()

        #Remover duplicados
        duplicadas = df[df.duplicated()]        #print(dup) #301 linhas duplicadas
        df.drop_duplicates(inplace=True)
        duplicadas = df[df.duplicated()]

        #Verifica se a limpeza das duplicadas está correta
        assert duplicadas.empty

        #Transformar a coluna "Preco" em float
        df.Preco = df.Preco.str.replace('.','').str.replace(',','.').astype(float)

        #Verificar se existem valores nulos (NaN)
        #print(df.isnull().values.any()) # -- Nenhum valor nulo

        #Remover valores de preços que não fazem sentidos para o valor do produto
        """
            print(df.describe())
            count   465.000000
            mean   2091.038559
            std     586.069824
            min       6.000000
            25%    1950.000000
            50%    2129.000000
            75%    2290.000000
            max    9999.990000
        """
        #print(df.Preco.sort_values()) - alguns valores fora do padrão de preços
        #DELETAR VALORES MENORES QUE 1499.90 E MAIORES QUE 3199.00
        df = df[(df.Preco < 3199.00) & (df.Preco > 1499.90)]
        #print(df.Preco.sort_values()) - valores removidos

        self.final_data = df.copy()
        self.final_data.to_csv(self.clean_filename , index=False)

    def insertData(self):
        """
            FUNÇÃO AUXILIAR QUE CHAMA UM MÓDULO EM OUTRO SCRIPT (load_data.py)
        """
        commitData(self.clean_filename)

if __name__ == '__main__':
    app = App()