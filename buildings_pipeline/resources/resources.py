import logging
import pandas as pd
from dagster import resource, IOManager, io_manager, Field, MetadataValue
import os
from pathlib import Path

# IO Manager для обработки DataFrame
class DataFrameFileIOManager(IOManager):
    def __init__(self, base_dir):
        self.base_dir = base_dir
        # Создаем директорию, если она не существует
        os.makedirs(self.base_dir, exist_ok=True)
    
    def _get_path(self, context):
        # Формируем путь к файлу на основе имени актива
        return os.path.join(self.base_dir, f"{context.asset_key.path[-1]}.parquet")
    
    def handle_output(self, context, obj):
        if isinstance(obj, pd.DataFrame):
            # Записываем метаданные
            context.add_output_metadata({
                "row_count": len(obj),
                "column_count": len(obj.columns),
                "columns": MetadataValue.json(list(obj.columns)),
            })
            
            # Преобразуем проблемные столбцы в строковый тип
            # Вариант 1: преобразуем только столбец "Комментарии"
            if "Комментарии" in obj.columns:
                obj["Комментарии"] = obj["Комментарии"].astype(str)
            
            # Вариант 2: преобразуем все столбцы с типом 'object' в строковый тип
            # для безопасности (раскомментировать при необходимости)
            # for col in obj.select_dtypes(include=['object']).columns:
            #     obj[col] = obj[col].astype(str)
            
            # Сохраняем DataFrame в файл
            path = self._get_path(context)
            
            # Добавляем параметры для более безопасного сохранения
            obj.to_parquet(
                path,
                engine='pyarrow',
                index=False,
                # Можно также добавить:
                # allow_truncated_timestamps=True
            )
            
            context.add_output_metadata({
                "path": MetadataValue.path(path),
                "size_bytes": os.path.getsize(path),
            })

    
    def load_input(self, context):
        if not context.has_asset_key:
            return pd.DataFrame()
        
        # Получаем путь к файлу на основе asset_key
        asset_path = os.path.join(self.base_dir, f"{context.asset_key.path[-1]}.parquet")
        
        # Проверяем существование файла и загружаем данные
        if os.path.exists(asset_path):
            try:
                return pd.read_parquet(asset_path)
            except Exception as e:
                logging.error(f"Ошибка при загрузке файла {asset_path}: {e}")
                return pd.DataFrame()
        else:
            logging.warning(f"Файл {asset_path} не найден")
            return pd.DataFrame()

@io_manager(config_schema={"base_dir": Field(str, default_value="./data")})
def dataframe_file_io_manager(init_context):
    return DataFrameFileIOManager(init_context.resource_config["base_dir"])

# Ресурс логирования
@resource(config_schema={"log_level": str})
def logging_resource(init_context):
    log_level = init_context.resource_config.get("log_level", "INFO")
    level = getattr(logging, log_level)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('data_pipeline.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("data_pipeline")





@resource(config_schema={"base_dir": str})
def data_directory_resource(context):
    """Ресурс для управления директориями данных проекта."""
    base_dir = Path(context.resource_config["base_dir"])
    
    # Создаем структуру директорий
    raw_dir = base_dir / "raw"
    processed_dir = base_dir / "processed"
    output_dir = base_dir / "output"
    
    for directory in [base_dir, raw_dir, processed_dir, output_dir]:
        directory.mkdir(exist_ok=True, parents=True)
    
    return {
        "base": base_dir,
        "raw": raw_dir,
        "processed": processed_dir,
        "output": output_dir
    }
