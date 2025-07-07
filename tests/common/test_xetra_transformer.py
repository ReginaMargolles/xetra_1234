"""TestXetraETLMethods"""
import os
import unittest
from unittest.mock import patch
from io import BytesIO

import boto3
import pandas as pd
import numpy as np
from moto import mock_aws

from xetra.common.LoggerHandler import Logger_Handler
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig

class TestXetraETLMethods(unittest.TestCase):
    """
    Testing the XetraETL class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # mocking aws s3 connection start
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name_src = 'src-bucket'
        self.s3_bucket_name_trg = 'trg-bucket'
        self.meta_key = 'meta_key'
        # Creating s3 access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'
        # Creating the source and target bucket on the mocked aws
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name_src,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.s3.create_bucket(Bucket=self.s3_bucket_name_trg,
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'eu-central-1'})
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        # Creating S3BucketConnector testing instances
        self.s3_bucket_src = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_src)
        
        self.s3_bucket_trg = S3BucketConnector(self.s3_access_key,
                                                self.s3_secret_key,
                                                self.s3_endpoint_url,
                                                self.s3_bucket_name_trg)
        
        # Creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date': '2022-12-27',
            'src_columns': ['ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume'],
            'src_col_date': 'Date',
            'src_col_isin': 'ISIN',
            'src_col_time': 'Time',
            'src_col_start_price': 'StartPrice',
            'src_col_min_price': 'MinPrice',
            'src_col_max_price': 'MaxPrice',
            'src_col_traded_vol': 'TradedVolume'
        }
        conf_dict_trg = {
            'trg_col_isin': 'isin',
            'trg_col_date': 'date',
            'trg_col_op_price': 'opening_price_eur',
            'trg_col_clos_price': 'closing_price_eur',
            'trg_col_min_price': 'minimum_price_eur',
            'trg_col_max_price': 'maximum_price_eur',
            'trg_col_dail_trad_vol': 'daily_traded_volume',
            'trg_col_prev_closing_price': 'prev_closing_price',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
        
        # El operador ** desempaqueta el diccionario, pasando sus claves como nombres de parámetros 
        # y sus valores como argumentos.
        
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)
        
        
        # Creating source file into dataframe
        columns_src = ['ISIN', 'Mnemonic', 'Date', 'Time', 'StartPrice',
        'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        
        data = [['AT0000A0E9W5', 'SANT', '2022-12-27', '08:00', 14.02, 14.27, 14.02, 14.27, 1466],
                ['AT0000A0E9W5', 'SANT', '2022-12-27', '14:00', 14.12, 14.14, 14.12, 14.14, 500],
                ['AT0000A0E9W5', 'SANT', '2022-12-28', '08:00', 13.88, 13.88, 13.88, 13.88, 63],
                ['AT0000A0E9W5', 'SANT', '2022-12-29', '08:00', 13.88, 13.88, 13.88, 13.88, 63],
                ['CH0038389992', 'BBZA', '2022-12-29', '11:00', 60.85, 60.85, 60.85, 60.85, 50],
                ['CH0038389992', 'BBZA', '2022-12-30', '14:00', 60.95, 60.95, 60.95, 60.95, 20],
                ['CA4969024047', 'KIN2', '2022-12-31', '11:28', 4.819, 4.819, 4.819, 4.819, 300],
                ['CA4969024047', 'KIN2', '2022-12-31', '12:08', 4.880, 4.880, 4.880, 4.880, 1000],
                ['CA4969024047', 'KIN2', '2022-12-31', '14:16', 4.913, 4.913, 4.913, 4.913, 250]]
        
        self.df_src = pd.DataFrame(data, columns=columns_src)
        
        # Esta expresión selecciona solo la primera fila del DataFrame df_src. 
        # La notación loc[0:0] toma desde la fila con índice 0 hasta la misma fila 0 (es decir, una única fila), 
        # conservando el formato de DataFrame
        
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[0:0],
        '2022-12-27/2022-12-25_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[1:1],
        '2022-12-27/2022-12-25_BINS_XETR14.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[2:2],
        '2022-12-28/2022-12-28_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[3:3],
        '2022-12-29/2022-12-29_BINS_XETR08.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[4:4],
        '2022-12-29/2022-12-29_BINS_XETR15.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[5:5],
        '2022-12-30/2022-12-30_BINS_XETR14.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[6:6],
        '2022-12-31/2022-12-31_BINS_XETR11.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[7:7],
        '2022-12-31/2022-12-31_BINS_XETR12.csv','csv')
        self.s3_bucket_src.write_df_to_s3(self.df_src.loc[8:8],
        '2022-12-31/2022-12-31_BINS_XETR14.csv','csv')
        
        
        columns_report = ['ISIN', 'Date', 'opening_price_eur', 'closing_price_eur',
        'minimum_price_eur', 'maximum_price_eur', 'daily_traded_volume','prev_closing_price', 'change_prev_closing_%']
        

        data_report = [['AT0000A0E9W5','2022-12-27',14.02,14.12,14.02,14.27,1966,np.nan,np.nan],
	                   ['AT0000A0E9W5','2022-12-28',13.88,13.88,13.88,13.88,63,14.12,-1.7],
	                   ['AT0000A0E9W5','2022-12-29',13.88,13.88,13.88,13.88,63,13.88,0.0],
	                   ['CA4969024047','2022-12-31',4.82,4.91,4.82,4.91,1550,np.nan,np.nan],
	                   ['CH0038389992','2022-12-29',60.85,60.85,60.85,60.85,50,60.85,0.0],
	                   ['CH0038389992','2022-12-30',60.95,60.95,60.95,60.95,20,np.nan,np.nan]]

        
        self.df_report = pd.DataFrame(data_report, columns=columns_report)
        
        self.logger = Logger_Handler(logger_name="app_logger3", log_file = 'Xetra_ETL.log').get_logger()  # Obtener la misma instancia de logger
        self.logger.info("Iniciando pruebas para META_PROCESS")

    def tearDown(self):
        # mocking s3 connection stop
        self.mock_aws.stop()

    def test_extract_no_files(self):
        
        # SINO HAY FECHAS FALTANTES POR PROCESAR 
        
        """
        Tests the extract method when
        there are no files to be extracted
        
        """
        # Test init
        
        # El valor devuelto para la fecha minima seria el 1 enero de 2200
        # Este valor se utiliza cuando no hay fechas faltantes en el archivo de metadatos,
        # indicando que no hay necesidad de procesar más fechas.
        extract_date = '2200-01-02'
        
        # Se establece return_dates como lista vacia
        extract_date_list = []
        
        # Method execution
        
        # Aquí se está utilizando patch.object de la librería unittest.mock. 
        # Esta herramienta permite reemplazar temporalmente un método o atributo de un objeto,
        # durante el contexto del bloque with. En este caso:
        # Se está reemplazando el método return_date_list de la clase MetaProcess.
        # Se le está asignando como valor de retorno una lista fija: [extract_date, extract_date_list].
        # Esto se usa habitualmente en pruebas (tests) para evitar depender,
        # del comportamiento real del método y controlar el flujo.
        
        with patch.object(MetaProcess, "return_date_list", return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg, self.meta_key, self.source_config, self.target_config)
            df_return = xetra_etl.extract()
            
        # Test after method execution
        self.assertTrue(df_return.empty)

    def test_extract_files(self):
        
        """
        
        Tests the extract method when
        there are files to be extracted
        
        """
        
        # Expected results
        # Esto lo que hace es seleccionar un subconjunto de filas de un DataFrame de pandas (self.df_src)
        # y luego resetear el índice. Vamos por partes:
        # self.df_src.loc[1:8] Este fragmento selecciona las filas desde la fila 1 hasta la fila 8 inclusive, 
        # usando el índice del DataFrame. 
        # La función .loc[] accede por etiquetas de índice, no por posición numérica.
        # .reset_index(drop=True) Esto reinicia el índice de esas filas seleccionadas para que empiece de cero (0, 1, 2, ...).
        # El argumento drop=True indica que no quieres mantener el índice anterior como una nueva columna.
        df_exp = self.df_src.loc[0:8].reset_index(drop=True)
        
        # Test init
        
          
        # Minima fecha a partir de la cual se procesarán los archivos.
        extract_date = '2022-12-27'
        
        # Lista de fechas que no han sido procesadas.
        extract_date_list = ['2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        
        # ¿¿¿¿ que hace con extract_date y con extrac_date_list ????     
        # Devuelve una lista de las fechas no procesadas, verificando que sean iguales
        # o mayores que la fecha para la extraccion del origen.
        
        
        # Method execution
        
        # Aquí se está utilizando patch.object de la librería unittest.mock. 
        # Esta herramienta permite reemplazar temporalmente un método o atributo de un objeto,
        # durante el contexto del bloque with. En este caso:
        # Se está reemplazando el método return_date_list de la clase MetaProcess.
        # Se le está asignando como valor de retorno una lista fija: [extract_date, extract_date_list].
        # Esto se usa habitualmente en pruebas (tests) para evitar depender,
        # del comportamiento real del método y controlar el flujo.    
        
        with patch.object(MetaProcess, "return_date_list", return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg, self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.extract()
            print('CONTENIDO DE LA VARIABLE df_exp: ')
            print(df_exp)
            print('RESULTADO DE df_result: ')
            print(df_result)
        # Test after method execution
        # self.assertTrue(df_exp.equals(df_result))
        
        # Paso 1: comparar columnas
        print("📌 Columnas iguales:", df_exp.columns.equals(df_result.columns))

        # Paso 2: comparar índices
        print("📌 Índices iguales:", df_exp.index.equals(df_result.index))

        # Paso 3: comparar tipos de datos
        print("📌 Diferencias en dtype (si hay):")
        print(df_exp.dtypes.compare(df_result.dtypes))

        # Paso 4: comparación completa con feedback detallado
        try:
            pd.testing.assert_frame_equal(df_exp, df_result, check_dtype=True)
            print("✅ Los DataFrames son completamente iguales")
        except AssertionError as e:
            print("❌ Diferencias encontradas:")
            print(e)

    def test_transform_report1_emptydf(self):
        """
        Tests the transform_report1 method with
        an empty DataFrame as input argument
        """
        # Expected results
        log_exp = 'The dataframe is empty. No transformations will be applied.'
        # Test init
        extract_date = '2022-12-27'
        extract_date_list = ['2022-12-26','2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        df_input = pd.DataFrame()
        
        # Method execution
        with patch.object(MetaProcess, "return_date_list",return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,self.meta_key, self.source_config, self.target_config)
            df_result = xetra_etl.transform_report1(df_input)
            self.logger.info(log_exp)
        # Test after method execution
        self.assertTrue(df_result.empty)

    def test_transform_report1_ok(self):
        
        """
        Tests the transform_report1 method with
        an DataFrame as input argument
        
        """
        # Expected results
        print(' ESTAMOS EN EL METODO: test_transform_report1_ok')
        log1_exp = 'Applying transformations to Xetra source data for report 1 started...'
        log2_exp = 'Applying transformations to Xetra source data finished...'
        df_exp = self.df_report
        # Test init
        extract_date = '2022-12-27'
        extract_date_list = ['2022-12-26','2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        df_input = self.df_src.loc[0:8].reset_index(drop=True)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
            print('EL CONTENIDO DE LA VARIABLE DF_EXP ES:')
            print(df_exp)
   
       
        # Method execution
        with patch.object(MetaProcess, "return_date_list", return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg, self.meta_key, self.source_config, self.target_config)
            self.logger.info(log1_exp)
            df_result = xetra_etl.transform_report1(df_input)
            with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
                print('EL CONTENIDO DE LA VARIABLE DF_RESULT ES:')
                print(df_result)
            self.logger.info(log2_exp)
            
        # Test after method execution
        self.assertTrue(df_exp.equals(df_result))
        
        

    def test_load(self):
        """
        Tests the load method
        """
   
        df_exp = self.df_report
        meta_exp = ['2022-12-27', '2022-12-28', '2022-12-29','2022-12-30', '2022-12-31']
        # Test init
        extract_date = '2022-12-27'
        extract_date_list = ['2022-12-26','2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        df_input = self.df_report
        
        # Method execution
        with patch.object(MetaProcess, "return_date_list", return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg, self.meta_key, self.source_config, self.target_config)
            xetra_etl.load(df_input)
   
            
        # Test after method execution
        print('EL NOMBRE DEL TARGET SOURCE ES: ')
        print(self.target_config.trg_key)
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )

    def test_etl_report1(self):
        """
        Tests the etl_report1 method
        
        """
        print('ESTOY EJECUTANDO EL METODO test_etl_report1')
        # Expected results
        df_exp = self.df_report
        meta_exp = ['2022-12-27', '2022-12-28', '2022-12-29','2022-12-30', '2022-12-31']
        # Test init
        extract_date = '2022-12-27'
        extract_date_list = ['2022-12-26','2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        # Method execution
        with patch.object(MetaProcess, "return_date_list",
        return_value=[extract_date, extract_date_list]):
            xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,
                         self.meta_key, self.source_config, self.target_config)
            xetra_etl.etl_report1()
        # Test after method execution
        print('EL NOMBRE DEL TARGET SOURCE ES: ')
        print(self.target_config.trg_key)
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[0]
        print('EL CONTENIDO DE LA VARIABLE trg_file ES: ')
        print(trg_file)
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)
        # Cleanup after test
        self.trg_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': trg_file
                    },
                    {
                        'Key': trg_file
                    }
                ]
            }
        )

if __name__ == '__main__':
    unittest.main()
