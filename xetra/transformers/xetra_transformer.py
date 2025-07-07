"""Xetra ETL Component"""
import logging
from datetime import datetime
from typing import NamedTuple

import pandas as pd

from xetra.common.LoggerHandler import Logger_Handler
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess

class XetraSourceConfig(NamedTuple):
    
    # La clase NamedTuple en Python es una forma eficiente de definir estructuras de datos inmutables
    # con nombres de campo, lo que facilita el acceso a los valores sin necesidad de índices numéricos.
    # Se usa comúnmente para representar datos estructurados de manera clara y eficiente.
    
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_prev_closing_price: columna que se añade por las discrepancias en los resultados
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_prev_closing_price: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str

class XetraETL():
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
          
        self.logger = Logger_Handler(logger_name="app_logger3", log_file = 'Xetra_ETL.log', level=logging.INFO).get_logger()
        self.logger.info("Logger inicializado para pruebas.")
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args 
        self.trg_args = trg_args
        
          
        # Devuelve una lista de las fechas no procesadas desde (src_first_extract_date
        # determina la fecha para la extraccion del origen hasta la fecha actual
        
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.src_args.src_first_extract_date, self.meta_key, self.s3_bucket_trg)
        
        # Devuelve una lista de las fechas no procesadas, verificando que sean iguales
        # o mayores que la fecha para la extraccion del origen
        
        self.meta_update_list = [date for date in self.extract_date_list\
            if date >= self.extract_date]
        
        

    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame

        :returns:
          data_frame: Pandas DataFrame with the extracted data
        """
        self.logger.info('Extracting Xetra source files started...')
        
        print('LISTA DE FECHAS A PROCESAR self.extract_date_list: ')
        print(self.extract_date_list)
        
        files = [key for date in self.extract_date_list\
                     for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_src.read_csv_to_df(file)\
                for file in files], ignore_index=True)
            
        self.logger.info('Extracting Xetra source files finished.')
        return data_frame

    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applies the necessary transformation to create report 1

        :param data_frame: Pandas DataFrame as Input

        :returns:
          data_frame: Transformed Pandas DataFrame as Output
        """
        if data_frame.empty:
            self.logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        
        self.logger.info('Applying transformations to Xetra source data for report 1 started...')
        
        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        
        # Removing rows with missing values
        # Elimina las filas con valores nulos NaN
        data_frame.dropna(inplace=True)
        
        
        # Calculating opening price per ISIN and day
        data_frame[self.trg_args.trg_col_op_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                    .transform('first')
        data_frame[self.trg_args.trg_col_op_price]
        
        """
            WITH RankedData AS (
                SELECT 
                    src_col_isin,
                    src_col_date,
                    src_col_start_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY src_col_isin, src_col_date
                        ORDER BY src_col_time
                    ) AS rn
                FROM data_frame
            )
            
            UPDATE data_frame AS df
            SET trg_col_op_price = rd.src_col_start_price
            FROM RankedData AS rd
            WHERE df.src_col_isin = rd.src_col_isin
            AND df.src_col_date = rd.src_col_date
            AND rd.rn = 1;

        
        """            
                    
        
        # Calculating closing price per ISIN and day
        data_frame[self.trg_args.trg_col_clos_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
                .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
                        .transform('last')
                        
        """
            WITH RankedData AS (
            SELECT 
                src_col_isin,
                src_col_date,
                src_col_start_price,
                ROW_NUMBER() OVER (
                    PARTITION BY src_col_isin, src_col_date
                    ORDER BY src_col_time DESC
                ) AS rn
            FROM data_frame
            )
            UPDATE data_frame AS df
            SET trg_col_clos_price = rd.src_col_start_price
            FROM RankedData AS rd
            WHERE df.src_col_isin = rd.src_col_isin
            AND df.src_col_date = rd.src_col_date
            AND rd.rn = 1;
            
            SELECT 
                src_col_isin,
                src_col_date,
                LAST_VALUE(src_col_start_price) OVER (
                    PARTITION BY src_col_isin, src_col_date
                    ORDER BY src_col_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS trg_col_clos_price
            FROM data_frame;
        
        """
        # Renaming columns
        data_frame.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
            }, inplace=True)
        
        
        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        data_frame = data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
                .agg({
                    self.trg_args.trg_col_op_price: 'min',
                    self.trg_args.trg_col_clos_price: 'min',
                    self.trg_args.trg_col_min_price: 'min',
                    self.trg_args.trg_col_max_price: 'max',
                    self.trg_args.trg_col_dail_trad_vol: 'sum'})
                
        """
            SELECT 
                src_col_isin,
                src_col_date,
                MIN(trg_col_op_price) AS trg_col_op_price,
                MIN(trg_col_clos_price) AS trg_col_clos_price,
                MIN(trg_col_min_price) AS trg_col_min_price,
                MAX(trg_col_max_price) AS trg_col_max_price,
                SUM(trg_col_dail_trad_vol) AS trg_col_dail_trad_vol
            FROM data_frame
            GROUP BY src_col_isin, src_col_date;
            
            El parámetro as_index=False en groupby() en pandas indica que las columnas utilizadas para agrupar 
            (src_col_isin y src_col_date) no se usarán como índice en el DataFrame resultante. 
            En otras palabras, los valores de agrupación se mantendrán como columnas normales 
            en lugar de convertirse en el índice del DataFrame.
        
        """
        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        
        """
            Esta instrucción en pandas calcula el cambio porcentual respecto al cierre anterior de trg_col_op_price. 
            Lo hace en dos pasos:

            shift(1) → Desplaza los valores de trg_col_op_price una fila hacia abajo dentro de cada grupo src_col_isin. 
            Esto obtiene el valor del día anterior.

            Cálculo del cambio porcentual → Aplica la fórmula:

            ((precio actual - precio anterior) / precio anterior) x 100
            
            WITH PreviousClose AS (
                SELECT 
                    src_col_isin,
                    src_col_date,
                    trg_col_op_price,
                    LAG(trg_col_op_price) OVER (
                        PARTITION BY src_col_isin 
                        ORDER BY src_col_date
                    ) AS prev_clos_price
                FROM data_frame
            )
                SELECT 
                    src_col_isin,
                    src_col_date,
                    trg_col_op_price,
                    prev_clos_price,
                    CASE 
                        WHEN prev_clos_price IS NOT NULL 
                        THEN ((trg_col_op_price - prev_clos_price) / prev_clos_price) * 100
                        ELSE NULL
                    END AS trg_col_ch_prev_clos
                FROM PreviousClose;
        
        """
        
        data_frame[self.trg_args.trg_col_prev_closing_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_date]).reset_index(drop=True)\
                .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_clos_price]\
                    .shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            data_frame[self.trg_args.trg_col_clos_price] \
            - data_frame[self.trg_args.trg_col_prev_closing_price]
            ) / data_frame[self.trg_args.trg_col_prev_closing_price ] * 100
        # Rounding to 2 decimals
        data_frame = data_frame.round(decimals=2)
        # Removing the day before extract_date
        data_frame = data_frame[data_frame.Date >= self.extract_date].reset_index(drop=True)
        self.logger.info('Applying transformations to Xetra source data finished...')
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        
        """
        Saves a Pandas DataFrame to the target
        :param data_frame: Pandas DataFrame as Input
        
        """
        # Creating target key
        
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )
        
        # Writing to target
        
        self.s3_bucket_trg.write_df_to_s3(data_frame, target_key, self.trg_args.trg_format)
        self.logger.info('Xetra target data successfully written.')
        
        # Updating meta file
        
        MetaProcess.update_meta_file(self,self.meta_update_list, self.meta_key, self.s3_bucket_trg)
        self.logger.info('Xetra meta file successfully updated.')
        return True

    def etl_report1(self):
        
        """
        Extract, transform and load to create report 1
        """
        # Extraction
        data_frame = self.extract()
        
        # Transformation
        data_frame = self.transform_report1(data_frame)
        
        # Load
        self.load(data_frame)
        return True