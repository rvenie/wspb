import logging
import os
from dagster import resource, Field

class ColoredFormatter(logging.Formatter):
    """
    Форматер для раскрашивания логов в консоли
    """
    COLORS = {
        'DEBUG': '\033[94m',    # Синий
        'INFO': '\033[92m',     # Зеленый
        'WARNING': '\033[93m',  # Желтый
        'ERROR': '\033[91m',    # Красный
        'CRITICAL': '\033[1;91m', # Ярко-красный
        'RESET': '\033[0m'      # Сброс цвета
    }

    def __init__(self, fmt=None, datefmt=None, style='%'):
        super().__init__(fmt, datefmt, style)

    def format(self, record):
        log_message = super().format(record)
        level_name = record.levelname
        if level_name in self.COLORS:
            log_message = f"{self.COLORS[level_name]}{log_message}{self.COLORS['RESET']}"
        return log_message

def get_log_file_path(log_file_name):
    """
    Получает путь к файлу логов, создавая при необходимости директорию logs
    """
    logs_dir = "logs"
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    return os.path.join(logs_dir, log_file_name)

@resource(
    config_schema={
        "log_level": Field(
            str, 
            default_value="INFO", 
            description="Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
        ),
        "log_file": Field(
            str, 
            default_value="data_pipeline.log", 
            description="Имя файла для сохранения логов"
        ),
        "use_colored_console": Field(
            bool, 
            default_value=True, 
            description="Использовать цветной вывод в консоли"
        ),
        "log_format": Field(
            str, 
            default_value="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
            description="Формат сообщений лога"
        )
    }
)
def logging_resource(init_context):
    """
    Ресурс логирования для Dagster
    
    Предоставляет настраиваемый логгер с возможностью записи в файл и
    вывода в консоль с опциональным цветным форматированием.
    """
    log_level_str = init_context.resource_config.get("log_level", "INFO")
    log_file = init_context.resource_config.get("log_file", "data_pipeline.log")
    use_colored_console = init_context.resource_config.get("use_colored_console", True)
    log_format = init_context.resource_config.get("log_format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Преобразуем строковое представление уровня логирования в константу
    log_level = getattr(logging, log_level_str.upper())
    
    # Получаем полный путь к файлу логов
    log_file_path = get_log_file_path(log_file)
    
    # Создаем логгер
    logger = logging.getLogger("data_pipeline")
    logger.setLevel(log_level)
    
    # Удаляем существующие обработчики, если они есть
    if logger.handlers:
        logger.handlers.clear()
    
    # Создаем обработчик для записи в файл
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    
    # Создаем обработчик для вывода в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Настраиваем форматирование для консоли (с цветами или без)
    if use_colored_console:
        console_handler.setFormatter(ColoredFormatter(log_format))
    else:
        console_handler.setFormatter(logging.Formatter(log_format))
    
    logger.addHandler(console_handler)
    
    # Отключаем распространение логов на родительские логгеры
    logger.propagate = False
    
    logger.info(f"Логгер инициализирован с уровнем {log_level_str}")
    logger.info(f"Логи сохраняются в файл: {log_file_path}")
    
    return logger
