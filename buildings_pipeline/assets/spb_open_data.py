from dagster import asset, Field
import pandas as pd
import requests
import time
import zipfile
import warnings
from pathlib import Path
from urllib3.exceptions import InsecureRequestWarning
import os
# Игнорируем предупреждения InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

@asset(
    compute_kind="api",
    io_manager_key="dataframe_io_manager",
    config_schema={
        "dataset_name": Field(
            str,
            default_value="Технико-экономические паспорта многоквартирных домов", 
            description="Название набора данных для поиска"
        ),
        "structure_id": Field(
            int,
            default_value=207, 
            description="ID структуры данных"
        ),
        "verify_ssl": Field(
            bool,
            default_value=False, 
            description="Проверять SSL-сертификаты"
        ),
        "batch_size": Field(
            int,
            default_value=1000, 
            description="Размер страницы для получения данных"
        ),
        "save_interval": Field(
            int,
            default_value=5, 
            description="Интервал сохранения промежуточных результатов"
        ),
        "max_retries": Field(
            int,
            default_value=3, 
            description="Максимальное число повторных попыток при ошибке"
        ),
        "direct_download_url": Field(
            str,
            default_value="https://data.gov.spb.ru/irsi/7840013199-Tehniko-ekonomicheskie-pasporta-mnogokvartirnyh-domov/versions/6/export_data/", 
            description="URL для прямого скачивания архива с данными"
        )
    },
    group_name="buildings",
    description="Загрузка данных с портала открытых данных Санкт-Петербурга",
    required_resource_keys={"log", "data_dir"}
)
def spb_open_data(context) -> pd.DataFrame:
    logger = context.resources.log
    config = context.op_config
    data_dir = context.resources.data_dir
    
    # Настройка директорий для сохранения данных
    download_dir = data_dir["raw"] / "downloaded_datasets"
    temp_dir = data_dir["base"] / "temp_data"
    
    # Создаем директории
    download_dir.mkdir(exist_ok=True, parents=True)
    temp_dir.mkdir(exist_ok=True, parents=True)
    
    # Функции для работы с датасетом
    def download_dataset(url, output_dir):
        """
        Скачивает архив с данными по указанной ссылке
        """
        logger.info(f"Скачивание датасета с {url}")
        
        try:
            # Скачиваем архив
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, verify=False, stream=True)
            response.raise_for_status()
            
            # Получаем имя файла из заголовка Content-Disposition, если доступно
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition and 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[1].strip('"\'')
            else:
                # Иначе используем последнюю часть URL или генерируем имя
                filename = url.split('/')[-1]
                if not filename:
                    filename = f"dataset_{int(time.time())}.zip"
            
            file_path = output_dir / filename
            
            # Сохраняем файл
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Датасет успешно скачан и сохранен как {file_path}")
            return file_path
        
        except Exception as e:
            logger.error(f"Ошибка при скачивании датасета: {e}")
            return None

    def extract_zip(zip_path, extract_dir):
        """
        Распаковывает zip-архив
        """
        logger.info(f"Распаковка архива {zip_path}")
        
        try:
            extracted_files = []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Получаем список файлов в архиве
                file_list = zip_ref.namelist()
                
                # Распаковываем архив
                zip_ref.extractall(extract_dir)
                
                # Формируем полные пути к распакованным файлам
                extracted_files = [extract_dir / file for file in file_list]
            
            logger.info(f"Архив успешно распакован. Извлечено {len(extracted_files)} файлов")
            return extracted_files
        
        except Exception as e:
            logger.error(f"Ошибка при распаковке архива: {e}")
            return None

    def load_csv_data(csv_path):
        """
        Загружает данные из CSV-файла
        """
        logger.info(f"Загрузка данных из CSV-файла {csv_path}")
        
        try:
            # Пробуем разные кодировки
            encodings = ['utf-8', 'cp1251', 'latin1']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(csv_path, encoding=encoding)
                    logger.info(f"Данные успешно загружены из CSV с кодировкой {encoding}. Размер: {df.shape}")
                    return df
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при чтении CSV с кодировкой {encoding}: {e}")
                    continue
            
            logger.error(f"Не удалось прочитать CSV ни с одной из кодировок")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных из CSV: {e}")
            return None
    
    # Пробуем сначала напрямую скачать датасет
    logger.info(f"Попытка прямого скачивания датасета {config['dataset_name']}")
    
    try:
        # Скачиваем архив
        safe_name = config["dataset_name"].replace(' ', '_').lower()
        zip_path = download_dataset(config["direct_download_url"], download_dir)
        
        if zip_path:
            # Распаковываем архив
            extract_dir = download_dir / safe_name
            extract_dir.mkdir(exist_ok=True)
            extracted_files = extract_zip(zip_path, extract_dir)
            
            if extracted_files:
                # Ищем CSV файлы
                csv_files = [f for f in extracted_files if str(f).lower().endswith('.csv')]
                
                if csv_files:
                    # Загружаем данные из первого CSV
                    df = load_csv_data(csv_files[0])
                    
                    if df is not None:
                        logger.info(f"Данные успешно загружены из скачанного датасета. Размер: {df.shape}")
                        
                        # Сохраняем копию данных в CSV
                        output_file = data_dir["processed"] / f"{safe_name}.csv"
                        df.to_csv(output_file, index=False)
                        logger.info(f"Данные сохранены в файл {output_file}")
                        
                        return df
                    else:
                        logger.warning("Не удалось загрузить данные из скачанного CSV.")
                else:
                    logger.warning("В архиве не найдены CSV файлы.")
            else:
                logger.warning("Не удалось распаковать архив.")
        else:
            logger.warning("Не удалось скачать архив с данными.")
            
        logger.info("Переключаемся на получение данных через API...")
    
    except Exception as e:
        logger.error(f"Ошибка при прямом скачивании датасета: {e}")
        logger.info("Переключаемся на получение данных через API...")
    
    # Функция с повторными попытками для API
    def make_request_with_retry(url, headers, verify_ssl, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, verify=verify_ssl)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Ошибка запроса: {e}. Повторная попытка через {wait_time} сек...")
                    time.sleep(wait_time)
                else:
                    raise
    
    # Имя файла для промежуточных и финального результатов
    safe_name = config["dataset_name"].replace(' ', '_').lower()
    temp_file = temp_dir / f"{safe_name}_temp.csv"
    csv_filename = data_dir["processed"] / f"{safe_name}.csv"
    
    # Проверяем существование промежуточного файла для возможного продолжения загрузки
    all_results = []
    start_page = 1
    
    if temp_file.exists():
        try:
            temp_df = pd.read_csv(temp_file)
            all_results = temp_df.to_dict('records')
            start_page = (len(all_results) // config["batch_size"]) + 1
            logger.info(f"Найден промежуточный файл с {len(all_results)} записями. Продолжаем с страницы {start_page}")
        except Exception as e:
            logger.error(f"Ошибка при чтении промежуточного файла: {e}")
            logger.info("Начинаем загрузку с начала")
    
    try:
        # Получаем токен из переменных окружения или используем резервный
        api_token = os.environ.get("SPB_OPEN_DATA_TOKEN", "754869004361fb65d25b16169f664a231827b638")
        headers = {"Authorization": f"Token {api_token}"}
        
        # Базовый URL API
        base_url = "https://data.gov.spb.ru/api/v2"
        
        # Получаем ID набора данных, если еще не знаем
        dataset_id = None
        version_id = None
        
        if start_page == 1:  # Если начинаем с начала, ищем набор данных
            logger.info("Получение списка наборов данных...")
            
            # Получаем список наборов данных
            datasets = make_request_with_retry(f"{base_url}/datasets/?per_page=100", headers, config["verify_ssl"])
            
            # Ищем нужный набор данных
            for dataset in datasets['results']:
                if config["dataset_name"].lower() in dataset['name'].lower():
                    dataset_id = dataset['id']
                    logger.info(f"Найден набор данных '{dataset['name']}' с ID: {dataset_id}")
                    break
            
            # Проверяем следующие страницы, если набор не найден
            next_page = datasets.get('next')
            while next_page and not dataset_id:
                page_data = make_request_with_retry(next_page, headers, config["verify_ssl"])
                
                for dataset in page_data['results']:
                    if config["dataset_name"].lower() in dataset['name'].lower():
                        dataset_id = dataset['id']
                        logger.info(f"Найден набор данных '{dataset['name']}' с ID: {dataset_id}")
                        break
                
                next_page = page_data.get('next')
            
            if not dataset_id:
                logger.error(f"Набор данных '{config['dataset_name']}' не найден")
                return pd.DataFrame()
            
            # Получаем последнюю версию набора данных
            logger.info("Получение информации о последней версии...")
            version_data = make_request_with_retry(
                f"{base_url}/datasets/{dataset_id}/versions/latest/", 
                headers, 
                config["verify_ssl"]
            )
            version_id = version_data['id']
            logger.info(f"Последняя версия: {version_id}")
            
            # Проверяем наличие указанной структуры
            structure_exists = False
            for structure in version_data.get('structures', []):
                if structure['id'] == config["structure_id"]:
                    structure_exists = True
                    break
            
            if not structure_exists:
                logger.warning(f"Структура с ID {config['structure_id']} не найдена")
                if version_data.get('structures'):
                    structure_id = version_data['structures'][0]['id']
                    logger.info(f"Используем первую доступную структуру с ID {structure_id}")
                else:
                    logger.error("Не найдено ни одной доступной структуры")
                    return pd.DataFrame()
            
            # Сохраняем информацию для возможного последующего продолжения
            info_file = temp_dir / f"{safe_name}_info.txt"
            with open(info_file, "w") as f:
                f.write(f"{dataset_id},{version_id},{config['structure_id']}")
        else:
            # Восстанавливаем информацию из файла
            info_file = temp_dir / f"{safe_name}_info.txt"
            try:
                with open(info_file, "r") as f:
                    dataset_id, version_id, structure_id = f.read().split(",")
                logger.info(f"Восстановлена информация: dataset_id={dataset_id}, version_id={version_id}, structure_id={structure_id}")
            except Exception as e:
                logger.error(f"Ошибка восстановления информации: {e}")
                logger.info("Начинаем процесс заново")
                # Удаляем промежуточный файл
                if temp_file.exists():
                    temp_file.unlink()
                # Рекурсивно вызываем функцию
                return spb_open_data(context)
        
        # Если у нас уже есть промежуточные результаты, продолжаем загрузку с соответствующей страницы
        if start_page > 1:
            logger.info(f"Продолжаем загрузку с страницы {start_page}...")
            data_url = f"{base_url}/datasets/{dataset_id}/versions/{version_id}/data/{config['structure_id']}/?per_page={config['batch_size']}&page={start_page}"
            data = make_request_with_retry(data_url, headers, config["verify_ssl"])
            next_page = data.get('next')
        else:
            # Начинаем загрузку данных с первой страницы
            logger.info(f"Загрузка данных для структуры {config['structure_id']}...")
            data_url = f"{base_url}/datasets/{dataset_id}/versions/{version_id}/data/{config['structure_id']}/?per_page={config['batch_size']}"
            data = make_request_with_retry(data_url, headers, config["verify_ssl"])
            
            all_results = data['results']
            records_count = len(all_results)
            logger.info(f"Загружено {records_count} записей")
            
            # Сохраняем первый пакет данных
            df_temp = pd.DataFrame(all_results)
            df_temp.to_csv(temp_file, index=False)
            logger.info(f"Промежуточные результаты сохранены ({records_count} записей)")
            
            next_page = data.get('next')
        
        # Обрабатываем пагинацию
        page_num = start_page
        while next_page:
            page_num += 1
            
            logger.info(f"Загрузка страницы {page_num}...")
            try:
                next_data = make_request_with_retry(next_page, headers, config["verify_ssl"])
                page_results = next_data['results']
                all_results.extend(page_results)
                records_count = len(all_results)
                logger.info(f"Загружено еще {len(page_results)} записей (всего: {records_count})")
                
                # Сохраняем промежуточные результаты через указанное количество страниц
                if page_num % config["save_interval"] == 0:
                    df_temp = pd.DataFrame(all_results)
                    df_temp.to_csv(temp_file, index=False)
                    logger.info(f"Промежуточные результаты сохранены ({records_count} записей)")
                
                next_page = next_data.get('next')
            except Exception as e:
                logger.error(f"Ошибка при загрузке страницы {page_num}: {e}")
                logger.info("Сохраняем промежуточные результаты и пытаемся продолжить...")
                
                # Сохраняем то, что успели загрузить
                df_temp = pd.DataFrame(all_results)
                df_temp.to_csv(temp_file, index=False)
                
                # Пауза перед повторной попыткой
                time.sleep(5)
                
                # Пытаемся продолжить с текущей страницы
                try:
                    retry_url = f"{base_url}/datasets/{dataset_id}/versions/{version_id}/data/{config['structure_id']}/?per_page={config['batch_size']}&page={page_num}"
                    retry_data = make_request_with_retry(retry_url, headers, config["verify_ssl"], max_retries=config["max_retries"])
                    next_page = retry_data.get('next')
                    
                    # Если успешно получили данные, добавляем их
                    if 'results' in retry_data:
                        page_results = retry_data['results']
                        all_results.extend(page_results)
                        logger.info(f"Успешно восстановлена загрузка страницы {page_num}")
                except Exception as retry_e:
                    logger.error(f"Не удалось восстановить загрузку: {retry_e}")
                    logger.info("Завершаем с тем, что успели загрузить")
                    break
        
        # Создаем итоговый DataFrame
        df = pd.DataFrame(all_results)
        logger.info(f"Создан DataFrame размером {df.shape}")
        
        # Сохраняем итоговый результат
        df.to_csv(csv_filename, index=False)
        logger.info(f"Данные сохранены в файл {csv_filename}")
        
        # Удаляем промежуточные файлы (опционально)
        if temp_file.exists():
            temp_file.unlink()
        info_file = temp_dir / f"{safe_name}_info.txt"
        if info_file.exists():
            info_file.unlink()
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        # Сохраняем промежуточные результаты, если они есть
        if all_results:
            df_temp = pd.DataFrame(all_results)
            df_temp.to_csv(temp_file, index=False)
            logger.info(f"Сохранены промежуточные результаты ({len(all_results)} записей) в {temp_file}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {e}")
        if all_results:
            df_temp = pd.DataFrame(all_results)
            df_temp.to_csv(temp_file, index=False)
            logger.info(f"Сохранены промежуточные результаты ({len(all_results)} записей) в {temp_file}")
        return pd.DataFrame()
