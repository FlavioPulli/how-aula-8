import warnings
warnings.simplefilter("ignore")
import pandas as pd
import numpy as np
from dataclasses import dataclass
import re
from datetime import datetime,timedelta
from gbq.GbqMain import ToBigQuery


@dataclass
class Diretorio:
    path: str
    mes: str
    dia: str
    arquivo: str
    def concat(self):
        return f'{self.path}{self.mes}{self.dia}{self.arquivo}'

DataInicial = '01/05/2023'
Dia = datetime.now() - timedelta(days=1)
DiaFim = '31/05/2023' #Dia.strftime('%d-%m-%Y')

Entrada = Diretorio(r'C:\Users\flaviopulli\Documents)
Saida = Diretorio(r'C:\Users\flaviopulli\Documents)

credJson = r'C:\Users\flaviopulli\Documents\dadosConciliacao\code\ExtratoresBancos\gbq\FinancCredencial.json'

def Bb(arquivo, nmTabela, dtInicio, dtFim):
    filtro = ['Saldo Anterior           ']
    df = pd.read_excel(arquivo, skiprows=2)
    first_row_with_all_NaN = df[df.isnull().all(axis=1) == True].index.tolist()[0]
    df = df.loc[0:first_row_with_all_NaN-1]
    lan = pd.read_excel(Entrada.concat(), header=None, nrows=3)
    df = df.assign(nmBanco='Banco do Brasil')
    df = df.assign(agencia=lan.iloc[1, 1])
    df = df.assign(conta=lan.iloc[1, 3])
    conditions = [
            df['Historico'].str.contains('Cielo', flags=re.IGNORECASE) == True
            ]
    choices = ['CIELO']
    df['tpLanc'] = np.select(conditions, choices)
    df.rename(columns = {'Data':'data',
                         'Numero Documento':'nuDocumento',
                         'Historico': 'nmLancamento',
                         'Crédito (R$)':'vlCredito',
                         'Débito (R$)': 'vlDebito'
                         }, inplace = True)
    df['vlCredito'] = df.apply(lambda x: x['Valor R$ '] if x['Inf.'] == 'C' else 0, axis=1)
    df['vlDebito'] = df.apply(lambda x: x['Valor R$ '] if x['Inf.'] == 'D' else 0, axis=1)
    df = df[['data', 'nmBanco', 'agencia', 'conta', 'nmLancamento', 'nuDocumento', 'tpLanc', 'vlCredito', 'vlDebito']]
    df = df.query('nmLancamento not in @filtro')
    # Selecionando datas que serão consideradas na tabela
    SelectData = (df['data'] >= dtInicio) & (df['data'] <= dtFim)
    df = df.loc[SelectData]
    df.to_csv(f'{nmTabela}', sep=';',index=None)
    df['data'] = df['data'].astype(str)
    df['nmBanco'] = df['nmBanco'].astype(str)
    df['agencia'] = df['agencia'].astype(str)
    df['conta'] = df['conta'].astype(str)
    df['nmLancamento'] = df['nmLancamento'].astype(str)
    df['nuDocumento'] = df['nuDocumento'].astype(str)
    df['tpLanc'] = df['tpLanc'].astype(str)
    df['vlCredito'] = df['vlCredito'].apply(str).str.replace(' ', '', regex = False)
    df['vlDebito'] = df['vlDebito'].apply(str).str.replace(' ', '', regex = False)
    df['vlCredito'] = df['vlCredito'].apply(str).str.replace('.', '', regex = False)
    df['vlDebito'] = df['vlDebito'].apply(str).str.replace('.', '', regex = False)
    return df


if __name__ == '__main__':
    dfEnvio = Bb(arquivo=Entrada.concat(), nmTabela=Saida.concat(), dtInicio= DataInicial, dtFim = DiaFim)
    #print(dfEnvio.head(100))
    ToBigQuery(credJson, 'conciliacao-365212', 'Financeiro.tbTabela', dfEnvio, 'append')
