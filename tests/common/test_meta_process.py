"""TestMetaProcessMethods"""
import os
import unittest
from io import StringIO
from datetime import datetime, timedelta

import boto3
import pandas as pd
from moto import mock_aws

from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.LoggerHandler import Logger_Handler

""" class MetaProcessFormat(Enum):
            META_DATE_FORMAT = '%y-%m-%d'
            META_PROCESS_DATE_FORMAT = '%y-%m-%d %H:%M:%S'
            META_SOURCE_DATE_COL = 'source_date'
            META_PROCESS_COL = 'datetime_of_processing'
            META_FILE_FORMAT = 'csv' """


class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the MetaProcess class.
    """
    @mock_aws
    def setUp(self):
        """
        Setting up the environment
        """
        # mocking s3 connection start
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a S3BucketConnector instance
        self.s3_bucket_meta = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name)
        
        ########## HASTA EL CODIGO ES IDENTICO AL METODO  setUp(self) DE LA CLASE TEST_S3.PY
        
        # Generando una lista de fechas para los últimos 8 días, 
        # formateadas según el valor de MetaProcessFormat.META_DATE_FORMAT.value
        
        self.dates = [(datetime.today().date() - timedelta(days=day))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]
        
        self.logger = Logger_Handler(logger_name="app_logger2", log_file = 'meta_process.log').get_logger()  # Obtener la misma instancia de logger
        self.logger.info("Iniciando pruebas para META_PROCESS")
        
    @mock_aws    
    def tearDown(self):
        # mocking s3 connection stop
        #Se utiliza para detener el entorno simulado de aws
        self.mock_aws.stop()
        
        

    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method
        when there is no meta file
        
        Prueba el método update_meta_file cuando no existe un archivo meta
        
        """
        # Expected results
        
        date_list_exp = ['2022-04-18', '2022-04-19']
        
        # Obtiene la fecha actual datetime.today().date()
        # Se coloca dentro de una lista [datetime.today().date()] 
        # en la cual habra un unico elemento (fecha de hoy)
        # Se colocara en la lista otro elemento que sera una copia 
        # de la fecha de hoy, tendra dos elementos. los cuales estarán duplicados.
        
        proc_date_list_exp = [datetime.today().date()] * 2
        
        # Test init
        # Define la clave del archivo como meta.csv cuando el real se llama meta_file.csv
        
        meta_key = 'meta.csv'
        
        # Method execution
        # Actualizacion del archivo meta file añadiendole las fechas que han sido procesadas
        # acompañadas de la fecha de hoy como fecha de procesamiento
        self.logger.info("Actualizacion del archivo meta file añadiendole las fechas que han sido procesadas\
                         acompañadas de la fecha de hoy como fecha de procesamiento")
        # acompañadas de la fecha de hoy como fecha de procesamiento")
        MetaProcess.update_meta_file(self, date_list_exp, meta_key, self.s3_bucket_meta)
        
        # Read meta file
        
        # Accede el archivo S3 identificado por el objeto key_exp => self.s3_bucket.Object(key=key_exp)
        # Obtiene los datos del archivo, accediendo al contenido en Body => .get().get('Body')
        # lo que devuelve los datos en formato binario (bytes) => .read()
        
        data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
        
         
        # Se crea un buffer de texto en memoria usando StringIO, lo que te permite tratar data 
        # como un archivo virtual en Python. Esto es especialmente útil cuando trabajas 
        # con datos cargados desde S3 y necesitas procesarlos sin guardarlos en disco.
        out_buffer = StringIO(data)
        
        # Aquí estás cargando los datos en un DataFrame de pandas desde out_buffer, 
        # que es un archivo virtual en memoria creado con StringIO
        df_meta_result = pd.read_csv(out_buffer)
        
        # Del archivo leido coloca en date_list_result las fechas que han sido procesadas en forma de lista
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        
        # Del archivo leido coloca en proc_date_list_result las fechas en las cuales se han procesado
        # los archivos correspondientes a las fechas procesadas obtenidas del fichero.
        # Convierte las fechas en una lista, a las fechas se las realiza un cast a datetime
        # quedandose unicamente con la fecha sin inculir la horoa
        # El \ indica que la línea de código continúa en la siguiente línea, manteniendo la legibilidad.
        # proc_date_list_result = list(
        #    pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value])\
        #    .dt.date
        #    )
        
        proc_date_list_result = list(pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date)
        # Test after method execution
        # date_list_exp = ['2022-04-18', '2022-04-19']
        # date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        # Sino existe el fichero meta que es el objetivo de este test, la verificacion dara fallo
        
        self.assertEqual(date_list_exp, date_list_result)
        
        
        # proc_date_list_exp = [datetime.today().date()] * 2
        #  proc_date_list_result = list(
        #    pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value])\ 
        #    .dt.date)
         # Sino existe el fichero meta que es el objetivo de este test, la verificacion dara fallo
         
        self.assertEqual(proc_date_list_exp, proc_date_list_result)
        
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
        

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method
        when the argument extract_date_list is empty
        
        Prueba el método update_meta_file cuando el argumento extract_date_list está vacío.
        
        """
        # Expected results
        return_exp = True  
        
        # Test init
        # Define una lista vacia
        date_list = []
        
        # Define la clave del archivo como meta.csv cuando el real se llama meta_file.csv
        meta_key = 'meta.csv'
        
        
        # Method execution
        
    
        # Ejecuta el metodo update_meta_file para una lista de fechas sin ningun elemento
        # Para un nombre de fichero meta inexistente
        # self.s3_bucket_meta tipo (S3BucketConnector) utilizado para leer el fichero en caso de que exista
        self.logger.info('The datalist is empty, and no file will be written')
        result = MetaProcess.update_meta_file(self, date_list, meta_key, self.s3_bucket_meta)
        self.assertEqual(return_exp, result)
        
        
        
    def test_update_meta_file_meta_file_ok(self):
            """
            Tests the update_meta_file method
            when there already a meta file
            """
            # Expected results
            date_list_old = ['2022-04-12', '2022-04-13']
            date_list_new = ['2022-04-18', '2022-04-19']
            date_list_exp = date_list_old + date_list_new
            proc_date_list_exp = [datetime.today().date()] * 4
            # Test init
            meta_key = 'meta.csv'
            meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
            )
            self.logger.info('Writing Meta file')
            self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
            # Method execution
            MetaProcess.update_meta_file(self, date_list_new, meta_key, self.s3_bucket_meta)
            # Read meta file
            self.logger.info('Reading Meta file')
            data = self.s3_bucket.Object(key=meta_key).get().get('Body').read().decode('utf-8')
            out_buffer = StringIO(data)
            df_meta_result = pd.read_csv(out_buffer)
            date_list_result = list(df_meta_result[
                MetaProcessFormat.META_SOURCE_DATE_COL.value])
            proc_date_list_result = list(pd.to_datetime(df_meta_result[
                MetaProcessFormat.META_PROCESS_COL.value])\
                    .dt.date)
            # Test after method execution
            self.logger.info('Check the result of test is correct')
            self.assertEqual(date_list_exp, date_list_result)
            self.assertEqual(proc_date_list_exp, proc_date_list_result)
            # Cleanup after test
            self.s3_bucket.delete_objects(
                Delete={
                    'Objects': [
                        {
                            'Key': meta_key
                        }
                    ]
                }
            )
        
    def test_update_meta_file_meta_file_wrong(self):
            """
            Tests the update_meta_file method
            when there is a wrong meta file
            """
            # Expected results
            date_list_old = ['2022-04-12', '2022-04-13']
            date_list_new = ['2022-04-18', '2022-04-19']
            # Test init
            meta_key = 'meta.csv'
            meta_content = (
            f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}\n'
            f'{date_list_old[1]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)}'
            )
            self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
            # Method execution
            with self.assertRaises(WrongMetaFileException):
                MetaProcess.update_meta_file(self, date_list_new, meta_key, self.s3_bucket_meta)
                self.logger.info('Update meta file is not possible, wrong meta file')
            # Cleanup after test
            self.s3_bucket.delete_objects(
                Delete={
                    'Objects': [
                        {
                            'Key': meta_key
                        }
                    ]
                }
            )

    def test_return_date_list_no_meta_file(self):
        """
        Tests the return_date_list method
        when there is no meta file
        """
        # Expected results
        
        # Genera una lista de los ultimos 4 dias
        
        date_list_exp = [
            (datetime.today().date() - timedelta(days=day))\
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)
            ]
        # Almacena la fecha de hace dos dias
        
        min_date_exp = (datetime.today().date() - timedelta(days=2))\
            .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            
        # Test init
        first_date = min_date_exp
        meta_key = 'meta.csv'
        # Method execution
        # Devuelve la fecha minima coincidente con la fecha pasada por parametro 'first_date'
        # y una lista de fechas desde la fecha minima hasta la del dia de hoy
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                         self.s3_bucket_meta)
        # Test after method execution
   
        
        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        """
        Tests the return_date_list method
        when there is a meta file
        """
        # Expected results
        # Seria una lista con la fecha de hoy -1 dia, -2 dias, -7 dias
        min_date_exp = [
          (datetime.today().date() - timedelta(days=1))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
          (datetime.today().date() - timedelta(days=2))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value),
          (datetime.today().date() - timedelta(days=7))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        ]
        # Seria una lista con
        # la fecha de hoy, la fecha de hoy -1 dia y la fecha de hoy -2 dias (3 fechas)
        # la fecha de hoy, la fecha de hoy -1 dia, la fecha de hoy -2 dias, la fecha de hoy -3 dias  (4 fechas)
        # la fecha de hoy, la fecha de hoy -1 dia, la fecha de hoy -2 dias ... la fecha de hoy -8 dias (9 fechas)
        date_list_exp = [
          [(datetime.today().date() - timedelta(days=day))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(3)],
          [(datetime.today().date() - timedelta(days=day))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)],
          [(datetime.today().date() - timedelta(days=day))\
              .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(9)]
          ]
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{self.dates[3]},{self.dates[0]}\n' #Como origen fecha coge la fecha de hace tres dias y como fecha procesamiento la de hoy
          f'{self.dates[4]},{self.dates[0]}'   #Como origen fecha coge la fecha de hace cuatro dias y como fecha procesamiento la de hoy
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [
          self.dates[1], # fecha de hace 1 dia
          self.dates[4], # fecha de hace 4 dias
          self.dates[7]  # fecha de hace 7 dias
        ]
        # Method execution
        # LLama a return date para que devuelva
        # min_date_return: La fecha de hace un 1 dia, date_list_return: la lista de fechas que no han sido procesadas hasta el dia de hoy
        # min_date_return: La fecha de hace 4 dias, date_list_return: la lista de fechas que no han sido procesadas hasta el dia de hoy
        # min_date_return: La fecha de hace 7 dias, date_list_return: la lista de fechas que no han sido procesadas hasta el dia de hoy
        
        for count, first_date in enumerate(first_date_list): #enumerate genera un contador count
            min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                             self.s3_bucket_meta)
            # Test after method execution
            self.assertEqual(set(date_list_exp[count]), set(date_list_return))
            self.assertEqual(min_date_exp[count], min_date_return)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )
        
        

    def test_return_date_list_meta_file_wrong(self):
        """
        Tests the return_date_list method
        when there is a wrong meta file
        """
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'wrong_column,{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{self.dates[3]},{self.dates[0]}\n'
          f'{self.dates[4]},{self.dates[0]}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]
        
        # Method execution
        # Un KeyError en Python ocurre cuando intentas acceder a una clave (key) que no existe dentro de un diccionario.
        # Es una excepción que indica que el programa esperaba encontrar una clave específica, pero no la encontró.
        
        with self.assertRaises(KeyError):
            MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_empty_date_list(self):
        """
        Tests the return_date_list method
        when there are no dates to be returned
        """
        # Expected results
        min_date_exp = '2200-01-01'
        date_list_exp = []
        # Test init
        meta_key = 'meta.csv'
        meta_content = (
          f'{MetaProcessFormat.META_SOURCE_DATE_COL.value},'
          f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
          f'{self.dates[0]},{self.dates[0]}\n'
          f'{self.dates[1]},{self.dates[0]}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]
        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key,
                                                                         self.s3_bucket_meta)
        # Test after method execution
        self.assertEqual(date_list_exp, date_list_return)
        self.assertEqual(min_date_exp, min_date_return)
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )       
        

        
        
        
        
        
        
        
        
if __name__ == '__main__':
    unittest.main()
        
       
     
        
        
        
        
    
    

