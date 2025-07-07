import logging

class Logger_Handler:
    
    _instance = None
    
    def __new__(cls, logger_name, log_file, level=logging.INFO):
        if cls._instance is None:
            cls._instance = super(Logger_Handler, cls).__new__(cls)
            cls._instance.__init__(logger_name, log_file, level)
        return cls._instance
    
    
    def __init__(self, logger_name, log_file, level=logging.INFO):
        """
        Clase para configurar el manejador de logs.

        :param log_file: Nombre del archivo de logs.
        :param level: Nivel de logging (por defecto INFO).
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)

        # Configurar el manejador de archivo
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)

        # Formato del log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
         # Configurar el manejador de consola
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Agregar el manejador al logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        """ Devuelve el logger configurado. """
        return self.logger

