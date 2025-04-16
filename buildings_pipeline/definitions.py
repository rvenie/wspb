import dotenv
from dagster import Definitions, EnvVar
import os

# Загружаем переменные окружения для безопасного хранения токенов
dotenv.load_dotenv()

# Импортируем активы
from buildings_pipeline.assets.citywalls_data import citywalls_data
from buildings_pipeline.assets.spb_open_data import spb_open_data
from buildings_pipeline.assets.combined_data import combined_buildings_data

# Импортируем ресурсы
from buildings_pipeline.resources.resources import dataframe_file_io_manager, data_directory_resource
from buildings_pipeline.resources.logging import logging_resource

# Определяем базовую директорию для данных
data_base_dir = os.getenv("DAGSTER_DATA_DIR", "./data")

# Определяем ресурсы для Dagster
resources = {
    "log": logging_resource.configured({
            "log_level": "INFO",
            "log_file": "buildings_data.log",
            "use_colored_console": True
        }),
    "data_dir": data_directory_resource.configured({
        "base_dir": data_base_dir  # Добавляем конфигурацию для data_dir
    }),
    "dataframe_io_manager": dataframe_file_io_manager.configured({
        "base_dir": data_base_dir
    })
}

# Определение Dagster для нашего приложения
defs = Definitions(
    assets=[citywalls_data, spb_open_data, combined_buildings_data],
    resources=resources
)
