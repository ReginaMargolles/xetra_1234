""" TestS3BucketConnectorMethods"""

from io import BytesIO, StringIO

import os
import unittest

import boto3

from moto import mock_aws # esto sugiere que esta trabajando con pruebas en Python 
                          # usando bibliotecas como moto para simular servicios de AWS
import pandas as pd

from xetra.common.LoggerHandler import Logger_Handler
from xetra.common.custom_exceptions import WrongFormatException
from xetra.common.s3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    
    """
   
    @mock_aws
    def setUp(self):
        """
        Setting up the environment
        
        """
        # self hace referencia al propio objeto dentro de una clase.
        
        
     
        
        
        self.mock_aws = mock_aws() # es un funcion que inicializa un entorno simulado en AWS
                                   # es posible usarla para pruebas sin afectar a recursos reales de AWS
     
        self.mock_aws.start() # inicia el entorno simulado en el que se pueden hacer pruebas con servicios
                              # de AWS sin tocar los recursos reales.
                              
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'        # Claves de accesos a AWS, en un entorno real lo mejor
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'    # seria manejar estas credenciales a traves de variables
                                                        # de entorno o un servicio seguro como AWS Secrets Manager
        
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com' # Especifica la URL de servicio S3 de la region indicada
        self.s3_bucket_name = 'test-bucket'             # Define el nombre del bucket S3 donde se almacenaran los archivos
        
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1' # Aqui estas configurando las claves de acceso de AWS en variables de entorno
        os.environ[self.s3_secret_key] = 'KEY2'
        
        # Creating a bucket on the mocked s3
        # Se esta creando un recurso S3, inicializa un recurso S3 con el endpoint personalizado
        self.s3= boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        
        #Se crea el bucket par la region central "eu-central-1"
        self.s3.create_bucket(Bucket=self.s3_bucket_name, CreateBucketConfiguration = {'LocationConstraint': 'eu-central-1'})
                                                         
        #Se crea una referencia al bucket de S3 mediante boto3.resource()                                                  
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        
        # Creating a testing instance
        # Inicializacion de la instancia de S3BucketConnector
        
        self.s3_bucket_conn = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name,
                                                )
        self.logger = Logger_Handler(logger_name="app_logger", log_file = 's3_connector.log').get_logger()  # Obtener la misma instancia de logger
        self.logger.info("Iniciando pruebas para S3BucketConnector")
        
  
    @mock_aws
    def tearDown(self):
        """
        Executing after unittests, ejecuta la limpieza despues de los tests
        
        """
        #Se utiliza para detener el entorno simulado de aws
        
        self.mock_aws.stop()
    
    def test_list_files_in_prefix_ok(self):
        
        """
        Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 Bucket
        """
        # Expected results
        # Define prefix
        prefix_exp = 'prefix/' #genera un prefijo ficticio para la prueba en el bucket S3 ficticio el cual
                               #sera concatenado con el nombre del archivo ficticio para mocked
                               
        # Create a two S3 object key
        
        # f'{prefix_exp}' se utiliza para crear cadenas formateadas. Estas permiten incluir variables o expresiones directamente
        # dentro de una cadena, lo que hace que el formato de texto sea más legible y eficiente.
        
        # f'{prefix_exp}test1.csv' inserta  una cadena estatica junto con la primera cadena dinamica.
        
        # El patrón f'{cadena_dinamica}cadena_estatica' es una aplicación típica de f-strings en Python. 
        
        # Aquí se utiliza para combinar una variable o expresión dinámica (cadena_dinamica) 
        # con un texto fijo o estático (cadena_estatica).
        
        key1_exp = f'{prefix_exp}test1.csv' 
        key2_exp = f'{prefix_exp}test2.csv'
        
        # Test init
        # Create a mock CSV file content
        # Contenido del csv
        # col1,col2
        # valA,valB      
    
        csv_content ="""col1,col2
        valA,valB"""
        
        # Uploads it to your S3 bucket withe the keys specified
        # Esta subiendo objetos al bucket de Amazon S3 usando boto3.resource()
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        
        # Listar archivos en el bucket
        self.s3_bucket = boto3.resource("s3").Bucket(self.s3_bucket_name)
        
        # Lista objetos con el método correcto
        files = [obj.key for obj in self.s3_bucket.objects.all() if obj.key.startswith("prefix/")]
        #print(files)  # Ver los archivos encontrados

   
        # Method execution
        # Listar los archivos S3 dentro de un prefijo especifico, lista los objetos de un determinado directorio logico
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        
        # Tests after method execution
        # Comprueba que la longitud es igual a 2, sino la prueba fallara y se notifica como error
        # Verifica que la cantidad de archivos listados sea exactamente 2
        self.assertEqual(len(list_result),2)
        # Verifica que key1_exp está presente en list_result. Si no está, la prueba fallará.
        # Verifica el que la ruta y nombre asignado al archivo en key1_exp este en la lista de archivos 
        # filtrada del bucket de prueba
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after tests
        # Se borran los archivos del bucket de prueba
        self.s3_bucket.delete_objects(
            Delete= {
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
            
        )
    
    
    
    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of a
        wrong or not existing prefix
        """
        # Verificamos que no haya archivos en S3 con el prefijo 'no-prefix/'
        
        # Expected results
        # Define prefix
        prefix_exp = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)
    

    def test_read_csv_to_df_ok(self):
        """
        Test the read_csv_to_df method for
        reading 1 .csv file from the mocked s3 bucket
        
        Este código es una prueba unitaria que asegura que el método read_csv_to_df() 
        carga correctamente los datos desde un archivo en S3 y que el contenido del DataFrame coincide con lo esperado.
        
        """
        
        # Expected results
        key_exp = 'test.csv' # Nombre esperado del archivo CSV
        col1_exp = 'col1'    # Nombres de columnas esperadas
        col2_exp = 'col2'
        val1_exp = 'val1'    # Valores para las columnas en la primera fila
        val2_exp = 'val2'
        
        # Mensaje en los logs tras leer el archivo
        
        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}' # Se construye el contenido del archivo CSV
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)      # Se guarda el archivo en el bucket S3
        
       
        
       # Method execution 
       
        self.logger.info(f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}')
        df_result = self.s3_bucket_conn.read_csv_to_df(key_exp) # Lee el archivo CSV y lo convierte en un dataframe
            
           
        # Test after method execution
        self.assertEqual(df_result.shape[0],1) # Se verifica que el dataframe tiene una fila
        self.assertEqual(df_result.shape[1],2) # Se verifica que el dataframe tiene dos columnas
        self.assertEqual(val1_exp, df_result[col1_exp][0]) # Se confirma que los valores val1_exp y val2_exp
        self.assertEqual(val2_exp, df_result[col2_exp][0]) # estan correctamente almacenados en las columnas del dataframe
        
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete= {
               'Objects': [
                   {
                       'Key': key_exp
                   }
               ] 
                
            }
        )
           
    def test_write_df_to_s3_empty(self):
        """
        Test the write_df_to_s3 method with
        an empty DataFrame as input
        
        """
        # Prueba unitaria que permite verificar que se esta manejando un data frame vacio
        
        
        return_exp = None 
        # Resultado esperado ninguno
        log_exp = 'The dataframe is empty! No file will be written'
        # Mensaje de log, el dataframe esta vacio, no se escribira ningun fichero
        df_empty = pd.DataFrame()
        # define un dataframe vacio
        key = 'key.csv'
        # Define el parametro key usados para guardar un archivo en S3
        file_format = 'csv'
        # Define el parametro file format para guardar un archivo en S3
        
        
        
        self.logger.info('The dataframe is empty! No file will be written')
        result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format)
            # Ejecuta el metodo write_df_to_s3 el cual deberia detectar que el dataframe 
            # esta vacio y no escribir nada en S3
            

      
        self.assertEqual(return_exp, result)
        # Comprueba que el valor devuelto por write_df_to_s3 sea igual a None
        
        
        
    def test_write_df_to_s3_csv(self):
        """
        Tests the write_df_to_s3 method
        if writing csv is successful
                
        """
        # Prueba unitaria que comprueba el dataframe es escrito 
        # en el bucket S3 en un archivo con formato csv
        
        # Expected results
        
        # Se espera que la escritura en S3 de archivo csv sea exitosa
        return_exp = True
        
        # Se esta creando un DataFrame de pandas con dos filas y dos columnas
        df_exp = pd.DataFrame([['A','B'],['C','D']], columns = ['col1','col2'])
        
        # Se asigna el nombre del archivo a escribir
        key_exp = 'test.csv'
        
        # Crea el mensaje para el log donde se indica que se esta escribiendo el archivo en 
        # la url/nombre_bucket/nombre del archivo
        log_exp = f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        
        # Indica el formato del archivo
        file_format = 'csv'
        
        
       
        self.logger.info(f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}')
        # Ejecuta el metodo write_df_to_s3 escribiendo el dataframe df_exp
        # con el nombre del archivo key_exp y el formato de fichero file_format
        result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
        
            
        # Test after method execution
        
        # Accede el archivo S3 identificado por el objeto key_exp => self.s3_bucket.Object(key=key_exp)
        # Obtiene los datos del archivo, accediendo al contenido en Body => .get().get('Body')
        # Lee el contenido y lo decodifica a formato utf-8 => .read().decode('utf-8')
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read().decode('utf-8')
        
        # Se crea un buffer de texto en memoria usando StringIO, lo que te permite tratar data 
        # como un archivo virtual en Python. Esto es especialmente útil cuando trabajas 
        # con datos cargados desde S3 y necesitas procesarlos sin guardarlos en disco.
        out_buffer = StringIO(data)
        
        # Aquí estás cargando los datos en un DataFrame de pandas desde out_buffer, 
        # que es un archivo virtual en memoria creado con StringIO
        df_result = pd.read_csv(out_buffer)
        
        # Aquí estás verificando que el valor retornado por write_df_to_s3()
        # coincida exactamente con return_exp
        self.assertEqual(return_exp, result)
        
        # Verificando que los dos DataFrames de pandas sean idénticos usando assertTrue() en una prueba unitaria
        self.assertTrue(df_exp.equals(df_result))
        
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete ={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )
        
    def test_write_df_to_s3_parquet(self):
        """
        Tests the write_df_to_s3 method
        if writing parquet is successful
        
        """
        # Prueba unitaria que comprueba el dataframe es escrito 
        # en el bucket S3 en un archivo con formato parquet
        
        # Expected results
        
        # Se espera que la escritura en S3 de archivo csv sea exitosa
        return_exp = True
        
        # Se esta creando un DataFrame de pandas con dos filas y dos columnas
        df_exp = pd.DataFrame([['A','B'], ['C','D']], columns = ['col1','col2'])
        
        # Se asigna el nombre del archivo a escribir
        key_exp = 'test.parquet'
        
        
        # Test init
        
        # Indica el formato del archivo
        file_format = 'parquet'
        
        # Method execution
        
        self.logger.info(f'Writing file to {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}')
        result = self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, file_format)
         
            
       
        # Test after method execution
          
        # Accede el archivo S3 identificado por el objeto key_exp => self.s3_bucket.Object(key=key_exp)
        # Obtiene los datos del archivo, accediendo al contenido en Body => .get().get('Body')
        # lo que devuelve los datos en formato binario (bytes) => .read()
        data = self.s3_bucket.Object(key=key_exp).get().get('Body').read()
        
        # Se crea un buffer de texto en memoria usando StringIO, lo que te permite tratar data 
        # como un archivo virtual en Python. Esto es especialmente útil cuando trabajas 
        # con datos cargados desde S3 y necesitas procesarlos sin guardarlos en disco.
        out_buffer = BytesIO(data)
        
        # Aquí estás cargando los datos en un DataFrame de pandas desde out_buffer, 
        # que es un archivo binario virtual alamcenado en memoria creado con BytesIO
        df_result = pd.read_parquet(out_buffer)
        
        # Aquí estás verificando que el valor retornado por write_df_to_s3()
        # coincida exactamente con return_exp
        self.assertEqual(return_exp, result)
        
        # Verificando que los dos DataFrames de pandas sean idénticos usando assertTrue() en una prueba unitaria
        self.assertTrue(df_exp.equals(df_result))
     
        # Cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }      
        )
        
    def test_write_df_to_s3_wrong_format(self):
        """
        Test the write_df_to_s3 method
        if a not supported format is given as argument
                
        """
        
        # Prueba unitaria para probar si realmente el archivo que se 
        # desea escribir al bucket S3 tiene un formato incorrecto.
        
        # Expected results
        # Se espera que la escritura en S3 de archivo csv sea exitosa
        return_exp = True
        
        # Se esta creando un DataFrame de pandas con dos filas y dos columnas
        df_exp = pd.DataFrame([['A','B'], ['C','D']], columns = ['col1','col2'])
        
        # Se asigna el nombre del archivo a escribir
        key_exp = 'test.parquet'
        
        # Simula un formato de archivo no valido (S3 solo admite formatos como CSV y PAQUET)
        format_exp = 'wrong_format'
        
              
        # Especifica que la función debería de lanzar la excepcion WrongFormatException
        # cuando detecte un formato invalido.
        
        exception_exp = WrongFormatException
            
                
    
        # Se verifica que el metodo write_df_to_s3 lance la excepcion definida
        # como WrongFormatException en caso de intentar almacenar en el bucket S3
        # un formato de archivo no soportado.
        with self.assertRaises(exception_exp):
             # Ejecuta el metodo write_df_to_s3 escribiendo el dataframe df_exp
             # con el nombre del archivo key_exp y el formato de fichero file_format
             self.logger.info(f'The file format {format_exp} is not supported to be written to S3!')
             self.s3_bucket_conn.write_df_to_s3(df_exp, key_exp, format_exp)
         
            
       
            
                
    

if __name__ == "__main__":
    print("INICIANDO PRUEBAS...")
    unittest.main()
    print("FINALIZANDO PRUEBAS...")
        
        
 
   #testIns = TestS3BucketConnectorMethods()
   #testIns.setUp()
   #testIns.test_list_files_in_prefix_ok()
   #testIns.tearDown()
   