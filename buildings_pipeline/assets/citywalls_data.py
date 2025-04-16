from dagster import asset, Field
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
from urllib.parse import urljoin
import os
from pathlib import Path
import re
import warnings
from urllib3.exceptions import InsecureRequestWarning

# Игнорируем предупреждения InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

@asset(
    output_required=False,
    compute_kind="web:scraping",
    io_manager_key="dataframe_io_manager",
    config_schema={
        "index_url": Field(
            str,
            default_value="https://www.citywalls.ru/street_index.html", 
            description="URL индекса улиц"
        ),
        "output_filename": Field(
            str,
            default_value="citywalls_streets_data.xlsx", 
            description="Имя файла для сохранения результатов"
        ),
        "max_execution_time": Field(
            int, 
            default_value=3600, 
            description="Максимальное время в секундах"),
        "checkpoint_interval": Field(
            int, 
            default_value=5, 
            description="Интервал чекпоинтов (улиц)")
    },
    group_name="buildings",
    description="Скрейпинг данных о зданиях с сайта citywalls.ru",
    required_resource_keys={"log", "data_dir"}
)
def citywalls_data(context) -> pd.DataFrame:
    logger = context.resources.log
    data_dir = context.resources.data_dir
    config = context.op_config
    
    # Создаем директории для сохранения данных, если они не существуют
    raw_dir = data_dir["raw"]
    checkpoint_dir = data_dir["base"] / "checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    
    # Пути к файлам
    output_path = raw_dir / config["output_filename"]
    checkpoint_path = checkpoint_dir / "citywalls_checkpoint.txt"
    
    # Функция для получения всех ссылок на улицы
    def get_street_links(url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, verify=False, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Страница {url} недоступна. Код ответа: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = []
            
            # Находим все ссылки на улицы
            street_links = soup.select('table a[href*="search-street"]')
            
            for link in street_links:
                street_url = link['href']
                # Если ссылка относительная, добавляем базовый URL
                if not street_url.startswith('http'):
                    street_url = urljoin('https://www.citywalls.ru/', street_url)
                
                street_name = link.text.strip()
                links.append((street_name, street_url))
                
            logger.info(f"Найдено {len(links)} улиц на странице {url}")
            return links
        
        except Exception as e:
            logger.error(f"Ошибка при извлечении ссылок на улицы: {e}")
            return []

    # Функция для скрейпинга данных с одной страницы улицы
    def scrape_street_page(url, street_name, retries=3):
        for attempt in range(retries):
            try:
                headers = {
                    'User-Agent': random.choice([
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
                    ])
                }
                response = requests.get(url, headers=headers, verify=False, timeout=10)
                if response.status_code != 200:
                    logger.warning(f"Страница {url} недоступна. Код ответа: {response.status_code}")
                    if attempt < retries - 1:
                        time.sleep(3**(attempt+1))
                        continue
                    return None
                
                soup = BeautifulSoup(response.content, 'html.parser')
                data = []
                
                # Получаем все блоки с домами
                houses = soup.find_all('div', class_='cssHouseHead')
                
                if not houses:
                    logger.warning(f"На странице {url} не найдены дома")
                    return None
                
                for house in houses:
                    try:
                        # Название дома
                        title_element = house.find('h2')
                        title = title_element.text.strip() if title_element else "Название не найдено"
                        
                        # Изображение
                        photo_element = house.find('div', class_='photo')
                        photo = ""
                        if photo_element and photo_element.find('img'):
                            photo_src = photo_element.find('img').get('src', '')
                            photo = urljoin('https://www.citywalls.ru/', photo_src) if photo_src else "Фото не найдено"
                        else:
                            photo = "Фото не найдено"
                        
                        # Адрес
                        address_element = house.find('div', class_='address')
                        address = address_element.text.strip() if address_element else "Адрес не найден"
                        
                        # Находим таблицу с информацией
                        table = house.find('table')
                        
                        architects = "Не указаны"
                        year = "Не указан"
                        style = "Не указан"
                        
                        if table:
                            rows = table.find_all('tr')
                            
                            for row in rows:
                                item = row.find('td', class_='item')
                                value = row.find('td', class_='value')
                                
                                if item and value:
                                    item_text = item.text.strip()
                                    value_text = value.text.strip()
                                    
                                    if "Архитекторы" in item_text:
                                        architects = value_text
                                    elif "Год постройки" in item_text:
                                        year = value_text
                                    elif "Стиль" in item_text:
                                        style = value_text
                        
                        # Комментарии
                        comments_element = house.find('a', class_='imb_comm')
                        comments = comments_element.text.strip() if comments_element else "0"
                        
                        # Ссылка на страницу дома
                        house_link_element = house.find('a', href=True)
                        house_link = ""
                        if house_link_element:
                            house_href = house_link_element.get('href', '')
                            house_link = urljoin('https://www.citywalls.ru/', house_href) if house_href else ""
                        
                        data.append({
                            'Улица': street_name,
                            'Название': title,
                            'Фото': photo,
                            'Адрес': address,
                            'Архитекторы': architects,
                            'Год постройки': year,
                            'Стиль': style,
                            'Комментарии': comments,
                            'Ссылка': house_link
                        })
                    except Exception as e:
                        logger.error(f"Ошибка при обработке дома на странице {url}: {e}")
                        continue
                
                logger.info(f"Получено {len(data)} зданий с улицы '{street_name}'")
                return data
            
            except Exception as e:
                logger.error(f"Попытка {attempt+1}/{retries}: Ошибка при обработке страницы {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(3**(attempt+1))
                else:
                    logger.error(f"Не удалось обработать страницу {url} после {retries} попыток")
                    return None

    # Функция для обработки пагинации
    def process_pagination(base_url, street_name):
        all_data = []
        page = 1
        
        # Для отслеживания повторяющихся данных
        last_buildings = []
        same_size_count = 0
        max_same_size = 3  # Максимальное допустимое количество страниц с одинаковым количеством данных
        
        while True:
            # Формируем URL страницы
            if page == 1:
                url = base_url
            else:
                street_id_match = re.search(r'search-street(\d+)', base_url)
                if street_id_match:
                    street_id = street_id_match.group(1)
                    url = f'https://www.citywalls.ru/search-street{street_id}-page{page}.html'
                else:
                    logger.error(f"Не удалось извлечь ID улицы из URL: {base_url}")
                    break
            
            logger.info(f"Обрабатываем страницу {page} для улицы '{street_name}': {url}")
            
            try:
                # Получаем данные с текущей страницы
                page_data = scrape_street_page(url, street_name)
                
                if not page_data or len(page_data) == 0:
                    logger.info(f"На странице {url} нет данных о домах или страница не существует")
                    break
                
                # Проверка на зацикливание - получаем ли мы одинаковое количество данных
                current_size = len(page_data)
                
                # Если у нас одинаковое количество зданий на нескольких страницах подряд
                if last_buildings and current_size == len(last_buildings[-1]):
                    same_size_count += 1
                    
                    # Дополнительная проверка: совпадают ли адреса зданий
                    current_addresses = [b['Адрес'] for b in page_data]
                    last_addresses = [b['Адрес'] for b in last_buildings[-1]]
                    
                    addresses_match = set(current_addresses) == set(last_addresses)
                    
                    if addresses_match and same_size_count >= max_same_size:
                        logger.warning(f"Обнаружено зацикливание! {same_size_count} страниц подряд содержат одинаковые данные")
                        break
                else:
                    same_size_count = 0
                
                # Сохраняем данные последней страницы для сравнения
                last_buildings.append(page_data)
                if len(last_buildings) > max_same_size:
                    last_buildings.pop(0)
                
                all_data.extend(page_data)
                logger.info(f"Всего собрано {len(all_data)} зданий для улицы '{street_name}'")
                
                # Переходим к следующей странице
                page += 1
                
                # Добавляем задержку
                delay = 1 + random.random() * 3
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"Ошибка при обработке пагинации для улицы '{street_name}': {e}")
                break
        
        return all_data

    # Функция для сохранения прогресса
    def save_checkpoint(street_name):
        checkpoint_path.parent.mkdir(exist_ok=True)
        with open(checkpoint_path, 'w', encoding='utf-8') as f:
            f.write(street_name)
        logger.info(f"Сохранена точка возобновления: '{street_name}'")

    # Функция для возобновления скрейпинга с сохраненного состояния
    def resume_scraping():
        if checkpoint_path.exists():
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                last_street = f.read().strip()
                logger.info(f"Найдена точка возобновления: '{last_street}'")
                return last_street
        
        return None

    # Функция для скрейпинга всех улиц
    def scrape_all_streets():
        all_data = []
        start_time = time.time()
        max_time = config["max_execution_time"]
        
        # Загружаем существующие данные, если они есть
        if output_path.exists():
            try:
                existing_df = pd.read_excel(output_path)
                all_data = existing_df.to_dict('records')
                logger.info(f"Загружены существующие данные: {len(all_data)} записей из {output_path}")
            except Exception as e:
                logger.error(f"Ошибка при загрузке существующих данных: {e}")
        
        # Получаем последний чекпоинт
        last_street = resume_scraping()
        resume_mode = last_street is not None
        
        # Получаем ссылки на улицы
        street_links = get_street_links(config["index_url"])
        if not street_links:
            logger.warning("Не удалось получить ссылки на улицы")
            return all_data  # Возвращаем что есть, не останавливаем пайплайн
        
        # Обрабатываем каждую улицу с проверкой тайм-аута
        streets_processed = 0
        for i, (street_name, street_url) in enumerate(street_links, 1):
            # Проверяем тайм-аут
            if time.time() - start_time > max_time:
                logger.warning(f"Достигнут лимит времени ({max_time} сек). Приостанавливаем скрейпинг.")
                save_checkpoint(street_name)
                break
                
            # Проверяем режим возобновления
            if resume_mode and street_name != last_street:
                continue
            elif resume_mode and street_name == last_street:
                resume_mode = False
            
            try:
                # Получаем данные с учетом пагинации
                street_data = process_pagination(street_url, street_name)
                
                if street_data:
                    all_data.extend(street_data)
                    streets_processed += 1
                    
                    # Сохраняем промежуточные результаты
                    if streets_processed % config["checkpoint_interval"] == 0:
                        df = pd.DataFrame(all_data)
                        df.to_excel(output_path, index=False)
                        save_checkpoint(street_name)
                        logger.info(f"Сохранен чекпоинт для улицы '{street_name}'")
            except Exception as e:
                logger.error(f"Ошибка при обработке улицы '{street_name}': {e}")
                save_checkpoint(street_name)
                # Продолжаем следующую улицу, не останавливаемся
                continue
        
        return all_data

    # Основной код актива
    logger.info("Начинаем скрейпинг данных по улицам...")
    try:
        data = scrape_all_streets()
        
        if data:
            df = pd.DataFrame(data)
            df.to_excel(output_path, index=False)
            logger.info(f"Данные сохранены в файл {output_path}. Всего записей: {len(data)}.")
            return df            
        else:
            # Возвращаем пустой DataFrame вместо ошибки
            logger.warning("Нет данных для обработки.")
            return pd.DataFrame()
    except Exception as e:
            logger.error(f"Ошибка в активе citywalls_data: {e}")
            # Возвращаем пустой DataFrame чтобы не останавливать пайплайн
            return pd.DataFrame()
