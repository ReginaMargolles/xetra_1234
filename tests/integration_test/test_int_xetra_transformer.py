"""Integration Test XetraETLMethods"""
import os
import unittest
from io import BytesIO
from datetime import datetime, timedelta

import boto3
import numpy as np
import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig

class IntTestXetraETLMethods(unittest.TestCase):
    """
    Integration testing the XetraETL class.
    """

    def setUp(self):
        """
        Setting up the environment
        """
        # Defining the class arguments
        self.s3_access_key = 'AWS_ACCESS_KEY_ID'
        self.s3_secret_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.us-east-1.amazonaws.com'
        self.s3_bucket_name_src = 'xetra-int-test-src-regina'
        self.s3_bucket_name_trg = 'xetra-int-test-trg-regina'
        self.meta_key = 'meta_file.csv'
        # Creating the source and target bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
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
        # Creating a list of dates
        self.dates = ['2022-12-27', '2022-12-28', '2022-12-29', '2022-12-30', '2022-12-31']
        # Creating source and target configuration
        conf_dict_src = {
            'src_first_extract_date':'2022-12-27',
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
            'trg_col_prev_closing_price':'prev_closing_price',
            'trg_col_ch_prev_clos': 'change_prev_closing_%',
            'trg_key': 'report1/xetra_daily_report1_',
            'trg_key_date_format': '%Y%m%d_%H%M%S',
            'trg_format': 'parquet'
        }
       
        
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)
        # Creating source files on mocked s3
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

    def tearDown(self):
        for key in self.src_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()

    def test_int_etl_report1_no_metafile(self):
        """
        Integration test for the etl_report1 method
        """
        # Expected results
        df_exp = self.df_report
        print(self.dates)
        meta_exp = [self.dates[0],self.dates[1], self.dates[2], self.dates[3], self.dates[4]]
        print('CONTENIDO DE META_EXP')
        print(meta_exp)
        # Method execution
        xetra_etl = XetraETL(self.s3_bucket_src, self.s3_bucket_trg,
                             self.meta_key, self.source_config, self.target_config)
        xetra_etl.etl_report1()
        # Test after method execution
        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.trg_key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get('Body').read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result['source_date']), meta_exp)

if __name__ == '__main__':
    unittest.main()
