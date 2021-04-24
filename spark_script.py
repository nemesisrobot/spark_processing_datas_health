#Author:Diego Silva
#Date:23/04/2021
#Description:Script to collect datas and process analitics

import requests as rq


#class to collect datas
class CollectDatas:

    #constructo to init atributes
    def __init__(self, url):
        self._url = url
        self._pathbase = 'database\datas.csv'

    #method to save to datas
    def save_datas(self) -> None:

        #try to fix erro for connect
        try:
            datas = rq.get(url=self._url)

            #check code success 200
            if datas.status_code == 200:

                #create to .csv
                save_file = open(self._pathbase, 'w')
                lista_datas = ((datas.content).decode('utf-8')).split('\n')
                for x in lista_datas:
                    save_file.write('{}'.format(str(x)))
                save_file.close()
                
            else:
                print("Bad requestion")
        except Exception as e:
            print(e.args)

    #method get path
    def get_path_database(self) -> str:
        return self._pathbase


database = CollectDatas("https://sage.saude.gov.br/dados/repositorio/distribuicao_respiradores.csv")
database.save_datas()