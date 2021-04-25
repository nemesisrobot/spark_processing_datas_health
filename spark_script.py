#Author:Diego Silva
#Date:23/04/2021
#Description:Script to collect datas and process analitics

import requests as rq
import findspark

#init spart to processing
findspark.init()

#library pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, DateType
from pyspark.sql.functions import col, sum

from decimal import Decimal


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

#class to connect spark
class SparkConnectDatas:

    def __init__(self):
        self._master = None
        self._app = None

    def set_master(self, master) -> None:
        self._master = master

    def set_app(self, app) -> None:
        self._app = app

    def getSession(self):
        return SparkSession.builder.master(self._master).appName(self._app).getOrCreate()


#init class to get datas and create session to process with apache spark
database = CollectDatas("https://sage.saude.gov.br/dados/repositorio/distribuicao_respiradores.csv")
database.save_datas()


#setting connection with spark
connect_spark = SparkConnectDatas()
connect_spark.set_master('local[1]')
connect_spark.set_app('Health')

#get session and load datas
spark = connect_spark.getSession()
dataset = spark.read.csv(database.get_path_database(), header=True, sep=';')

#create temp view
dataset.createOrReplaceTempView('INSUMOS')

#clean datas NULL in column VALOR
dataset = dataset.filter("VALOR IS NOT NULL")
print(dataset.printSchema())

#cast in columns of type decimal and int
dataset = dataset.withColumn("VALOR",col("VALOR").cast(DecimalType()))\
    .withColumn("QUANTIDADE", col("QUANTIDADE").cast(IntegerType()))\
    .withColumn("DATA", col("DATA").cast(DateType()))\
    .withColumn("DATA DE ENTREGA", col("DATA DE ENTREGA").cast(DateType()))


#distinct kind of feature
types_list = dataset.select("TIPO").distinct().collect()

#Calculating spending by type
value_type=0

for x in types_list:
    value = spark.sql("SELECT VALOR FROM INSUMOS WHERE TIPO='{}'".format(x['TIPO']))
    for y in value.collect():
        value_type+=Decimal((str(y['VALOR']).replace('.', '')).replace(',', '.'))

    print('Valor Gasto Para {} foi de R${}'.format(x['TIPO'],value_type))



#close session
spark.stop()
