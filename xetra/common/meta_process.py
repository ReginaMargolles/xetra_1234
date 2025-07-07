"""
Methods for processing the meta file

"""

import collections
from datetime import datetime, timedelta
import logging
import pandas as pd
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
from xetra.common.s3 import S3BucketConnector
from xetra.common.LoggerHandler import Logger_Handler


class MetaProcess():

    """
     class for working with the meta file
    
    """
    def __init__(self):
        
        self.logger = Logger_Handler(logger_name="app_logger2", log_file = 'meta_proccess.log', level=logging.INFO).get_logger()
        self.logger.info("Logger inicializado para pruebas.")
         
     

   

    def update_meta_file(self, extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed date

        Params:
            extract_date_list (list) -> a list of dates that are extracted from the source
            metra_key (str) -> key of the meta file on the S3 bucket
            s3_bucket_meta (S3BucketConnector) -> for the bucket with the meta file
        """
        
        # Creating an empty DataFrame using the meta file column names
        # Recordemos por favor la clase MetaProcessFormat:
        """ class MetaProcessFormat(Enum):
            META_DATE_FORMAT = '%y-%m-%d'
            META_PROCESS_DATE_FORMAT = '%y-%m-%d %H:%M:%S'
            META_SOURCE_DATE_COL = 'source_date'
            META_PROCESS_COL = 'datetime_of_processing'
            META_FILE_FORMAT = 'csv' """
        
        # Se crea un dataframe con los valores de las columnas del fichero meta_file
        # no olvidemos los nombre originales 'source_date' y 'datetime_of_processing'
      
        df_new = pd.DataFrame(columns=[
            # La propiedad .value es caracteristica del tipo Enum en Python
            MetaProcessFormat.META_SOURCE_DATE_COL.value, 
            MetaProcessFormat.META_PROCESS_COL.value])
        
        # Filling the date column with extract_date_list
        # La fecha source_data sera involucrada a la hora de establecer el rango
        # de los ficheros csv procesados para determinar a traves de su prefijo cuyo significado
        # es una fecha representa el dia/mes/año en que los datos son tomados.
        # Se asignará a la columna META_SOURCE_DATE_COL (source_date) el valor del contenido
        # de la variable extract_date_list la cual toma valores a través de la logica en la cual
        # a través de un fecha establecida en la configuracion y source_date se calcula un conjunto
        # de fechas que seran las utilizadas como prefix para elegir el fichero csv a procesar
                
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        
        # Filling the processed column
        # Se asiginará a la columna META_PROCESS_COL (datetime_of_processing) que es igual
        # a la fecha en la que se invoca al procedimiento de procesamiento de csv.
       
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
            
        try:
            # If meta file exists -> union DataFrame of old and new meta data is created
            self.logger.info('The meta file exist, reading meta file csv to dataframe')
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
            
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # No meta file exits -> only the new data is used
            self.logger.info('The meta file not exist')
            df_all = df_new
            
        # Writing to S3
        self.logger.info('Writing meta file into bucket S3')
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True

        

    

    def return_date_list(first_date: str, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file

        returns:
          min_date: first date that should be processed
          return_date_list: list of all dates from min_date till today
        """
        # Convierte 'first_date' pasado como parametro, en un objeto fecha aplicandole
        # el formato META_DATE_FORMAT y restandole un dia
        start = datetime.strptime(first_date,MetaProcessFormat.META_DATE_FORMAT.value).date() - timedelta(days=1)
        # fecha actual 
        
        today_str = '2022-12-31'
        today = datetime.strptime(today_str, "%Y-%m-%d").date() 
                                     
        #today = datetime.today().date()
        
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            # Si el archivo existe se carga en un DataFrame
            df_meta = s3_bucket_meta.read_csv_to_df(meta_key)
            
            # Creating a list of dates from first_date untill today
            # Se genera una lista de fechas desde star hasta la fecha actual
            dates = [start + timedelta(days=x) for x in range(0, (today - start).days + 1)]
            
            
            # Creating set of all dates in meta file
            
            # Se obtienen las fechas ya procesadas y se almacenan en src_dates.
            src_dates = set(pd.to_datetime(
              df_meta[MetaProcessFormat.META_SOURCE_DATE_COL.value]
              ).dt.date)
            
            # Se identifican las fechas que aun no han sido procesadas en dates_missing
            dates_missing = set(dates[1:]) - src_dates
            
            # En caso de existir fechas faltantes
            if dates_missing:
                
                # Determining the earliest date that should be extracted
                # Determina la fecha mas antigua dentro de las fechas que no han sido procesada y le
                # resta un dia
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                
                # Creating a list of dates from min_date untill today
                # Devuelve la primera fecha que no ha sido procesada
                return_min_date = (min_date + timedelta(days=1))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                    
                # Devuelve una lista de las fechas no procesadas desde min_date hasta la fecha actual
                return_dates = [
                    date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                        for date in dates if date >= min_date
                        ]
            # SINO HAY FECHAS FALTANTES POR PROCESAR   
            else:
                # Setting values for the earliest date and the list of dates
                
                # Se establece return_dates como lista vacia
                return_dates = []
                
                # El valor devuelto para la fecha minima seria el 1 enero de 2200
                # Este valor se utiliza cuando no hay fechas faltantes en el archivo de metadatos,
                # indicando que no hay necesidad de procesar más fechas.
                
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                    
        # Captura la excepción que ocurre cuando el archivo de metadatos (meta_key),
        # no está presente en el bucket de Amazon S3.
        # Si no hay un archivo de metadatos, el código debe generar una lista de fechas sin depender de él.
                    
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # Si el archivo de metadatos no existe, el código genera una lista de fechas, 
            # desde first_date - 1 día hasta la fecha actual (today), 
            # asegurando que todas las fechas necesarias se procesen sin depender de datos previos.
           
            # Se asigna como fecha minima la fecha pasada por parametro en el método
            return_min_date = first_date
            
            # Recordemos que start es la fecha minima 'first_date' pasada como parametro
            # menos 1 dia.
            # Crea una lista de fechas desde start (que es first_date - 1 día) hasta today.
            
            return_dates = [
              (start + timedelta(days=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
              for x in range(0, (today - start).days + 1)
              ]
            
        return return_min_date, return_dates
    
    
   