from dagster import asset, Config
import pandas as pd
import numpy as np
import re
from pathlib import Path

class BuildingsConfig(Config):
    buildings_filename: str = "citywalls_streets_data.xlsx"
    open_data_filename: str = "технико-экономические_паспорта_многоквартирных_домов.csv"

@asset(
    compute_kind="transform",
    io_manager_key="dataframe_io_manager",
    group_name="buildings",
    description="Объединение данных о зданиях из разных источников",
    required_resource_keys={"log", "data_dir"}
)
def combined_buildings_data(
    context,
    config: BuildingsConfig
) -> pd.DataFrame:
    """Объединяет данные о зданиях из разных источников с использованием умного алгоритма слияния."""
    logger = context.log
    data_dir = context.resources.data_dir
    
    # Загрузка данных
    logger.info("Загрузка данных о зданиях...")
    try:
        buildings_path = data_dir["raw"] / config.buildings_filename
        buildings_data = pd.read_excel(buildings_path)
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных о зданиях: {e}")
        buildings_data = pd.DataFrame()
    
    logger.info("Загрузка данных из открытого портала...")
    try:
        open_data_path = data_dir["processed"] / config.open_data_filename
        try:
            open_data = pd.read_csv(open_data_path, encoding='utf-8')
        except:
            # Пробуем как Excel если CSV не удался
            open_data_path = data_dir["processed"] / config.open_data_filename.replace('.csv', '.xlsx')
            open_data = pd.read_excel(open_data_path)
    except Exception as e:
        logger.error(f"Ошибка при загрузке открытых данных: {e}")
        open_data = pd.DataFrame()
    
    # Проверка наличия данных
    if buildings_data.empty and open_data.empty:
        logger.error("Нет данных для обработки")
        return pd.DataFrame()
    
    # Предварительная нормализация полей адреса в buildings_data, если они отсутствуют
    logger.info("Предварительная обработка и нормализация данных...")
    
    # Функция для подготовки данных в стандартизированную форму
    def prepare_address_data(df, source_name):
        prepared_df = df.copy()
        
        # Определяем и нормализуем основные адресные поля
        # Для источника citywalls
        if source_name == 'citywalls':
            # Находим столбцы с информацией об адресе
            street_col = next((col for col in prepared_df.columns if 'улица' in col.lower()), None)
            if not street_col and 'Улица' in prepared_df.columns:
                street_col = 'Улица'
                
            house_col = next((col for col in prepared_df.columns if 'дом' in col.lower()), None)
            if not house_col and 'Дом' in prepared_df.columns:
                house_col = 'Дом'
            
            liter_col = next((col for col in prepared_df.columns if 'литер' in col.lower()), None)
            if not liter_col and 'Литера' in prepared_df.columns:
                liter_col = 'Литера'
            
            corpus_col = next((col for col in prepared_df.columns if 'корпус' in col.lower()), None)
            if not corpus_col and 'Корпус' in prepared_df.columns:
                corpus_col = 'Корпус'
            
            # Проверяем наличие полей и стандартизируем их имена
            if street_col and street_col != 'Улица':
                prepared_df['Улица'] = prepared_df[street_col]
            if house_col and house_col != 'Дом':
                prepared_df['Дом'] = prepared_df[house_col]
            if liter_col and liter_col != 'Литера':
                prepared_df['Литера'] = prepared_df[liter_col]
            if corpus_col and corpus_col != 'Корпус':
                prepared_df['Корпус'] = prepared_df[corpus_col]
                
            # Если есть полный адрес, но нет разбивки по полям
            if 'Адрес' in prepared_df.columns and (not 'Улица' in prepared_df.columns or not 'Дом' in prepared_df.columns):
                # Извлекаем компоненты адреса
                def extract_address_components(address):
                    if pd.isna(address) or not address:
                        return {'Улица': '', 'Дом': '', 'Корпус': '', 'Литера': ''}
                    
                    address = str(address).strip()
                    parts = re.split(r',\s*|\s+(?=\d+)', address, 1)
                    
                    result = {'Улица': '', 'Дом': '', 'Корпус': '', 'Литера': ''}
                    
                    if len(parts) >= 1:
                        result['Улица'] = parts[0].strip()
                    
                    if len(parts) >= 2:
                        house_part = parts[1].strip()
                        
                        # Извлекаем номер дома
                        house_match = re.search(r'^(?:дом\s*|д\.\s*)?(\d+[\w\/\-]*)', house_part)
                        if house_match:
                            result['Дом'] = house_match.group(1).strip()
                        
                        # Извлекаем литеру/корпус
                        liter_match = re.search(r'(?:литера?|лит\.?|л\.?)\s*([а-яА-Я\d]+)', house_part)
                        corpus_match = re.search(r'(?:корпус|корп\.?|к\.?)\s*([а-яА-Я\d]+)', house_part)
                        
                        if liter_match:
                            result['Литера'] = liter_match.group(1).strip()
                        if corpus_match:
                            result['Корпус'] = corpus_match.group(1).strip()
                    
                    return result
                
                # Применяем функцию к каждой строке
                address_components = prepared_df['Адрес'].apply(extract_address_components)
                
                # Заполняем отсутствующие поля
                for field in ['Улица', 'Дом', 'Корпус', 'Литера']:
                    if field not in prepared_df.columns:
                        prepared_df[field] = address_components.apply(lambda x: x[field])
        
        # Для источника opendata
        elif source_name == 'opendata':
            if 'Адрес' in prepared_df.columns:
                # Извлекаем компоненты адреса
                def extract_address_components(address):
                    if pd.isna(address) or not address:
                        return {'Улица': '', 'Дом': '', 'Корпус': '', 'Литера': ''}
                    
                    address = str(address).strip()
                    
                    # Удаляем префиксы города
                    address = re.sub(r'^(г\.|город|г\s+|санкт-петербург,\s*|\s*спб\s*,\s*|нп в составе спб\s*)', '', address, flags=re.IGNORECASE).strip()
                    
                    parts = re.split(r',\s*|\s+(?=\d+)', address, 1)
                    
                    result = {'Улица': '', 'Дом': '', 'Корпус': '', 'Литера': ''}
                    
                    if len(parts) >= 1:
                        result['Улица'] = parts[0].strip()
                    
                    if len(parts) >= 2:
                        house_part = parts[1].strip()
                        
                        # Извлекаем номер дома
                        house_match = re.search(r'^(?:дом\s*|д\.\s*)?(\d+[\w\/\-]*)', house_part)
                        if house_match:
                            result['Дом'] = house_match.group(1).strip()
                        
                        # Извлекаем литеру/корпус
                        liter_match = re.search(r'(?:литера?|лит\.?|л\.?)\s*([а-яА-Я\d]+)', house_part)
                        corpus_match = re.search(r'(?:корпус|корп\.?|к\.?)\s*([а-яА-Я\d]+)', house_part)
                        
                        if liter_match:
                            result['Литера'] = liter_match.group(1).strip()
                        if corpus_match:
                            result['Корпус'] = corpus_match.group(1).strip()
                    
                    return result
                
                # Применяем функцию к каждой строке
                address_components = prepared_df['Адрес'].apply(extract_address_components)
                
                # Заполняем отсутствующие поля
                for field in ['Улица', 'Дом', 'Корпус', 'Литера']:
                    if field not in prepared_df.columns:
                        prepared_df[field] = address_components.apply(lambda x: x[field])
        
        # Нормализация и очистка полей
        for field in ['Улица', 'Дом', 'Корпус', 'Литера']:
            if field in prepared_df.columns:
                prepared_df[field] = prepared_df[field].fillna('').astype(str).str.strip()
                if field == 'Литера' or field == 'Корпус':
                    prepared_df[field] = prepared_df[field].str.upper()
        
        return prepared_df
    
    # Подготавливаем данные
    citywalls_prepared = prepare_address_data(buildings_data, 'citywalls')
    opendata_prepared = prepare_address_data(open_data, 'opendata')
    
    # Применяем функцию умного объединения данных
    logger.info("Применение умного алгоритма объединения данных...")
    
    def merge_address_datasets(df1, df2):
        """
        Функция для умного объединения двух датафреймов с адресными данными
        """
        # Шаг 1: Подготовка данных
        df1 = df1.copy().reset_index(drop=True)
        df2 = df2.copy().reset_index(drop=True)
        
        # Добавляем идентификаторы
        df1['id_1'] = np.arange(len(df1))
        df2['id_2'] = np.arange(len(df2))
        
        # Создаем очищенные колонки для сравнения
        for df, prefix in [(df1, '1'), (df2, '2')]:
            for col in ['Улица', 'Дом', 'Корпус', 'Литера']:
                if col in df.columns:
                    df[f'{col}_clean'] = (df[col].fillna('')
                                        .str.lower()
                                        .str.strip()
                                        .str.replace(r'[^а-яa-z0-9]', '', regex=True))
        
        # Функция для пакетного слияния с отслеживанием объединенных записей
        def merge_and_track(left_df, right_df, on_cols, merge_name):
            merged = pd.merge(left_df, right_df, on=on_cols, how='inner', 
                            suffixes=('_citywalls', '_opendata'))
            merged['merge_type'] = merge_name
            return merged, set(merged['id_1']), set(merged['id_2'])
        
        # Шаг 2: Последовательное объединение
        # Хранение результатов всех объединений
        merge_results = []
        
        # Первое объединение: по всем полям
        merge_cols = [col for col in ['Улица_clean', 'Дом_clean', 'Корпус_clean', 'Литера_clean'] 
                      if col in df1.columns and col in df2.columns]
        
        if len(merge_cols) > 0:
            merged_all, ids1_all, ids2_all = merge_and_track(df1, df2, merge_cols, 'по_всем_полям')
            merge_results.append(merged_all)
            
            # Фильтрация необъединенных записей
            df1_remaining = df1[~df1['id_1'].isin(ids1_all)]
            df2_remaining = df2[~df2['id_2'].isin(ids2_all)]
        else:
            df1_remaining = df1.copy()
            df2_remaining = df2.copy()
            ids1_all = set()
            ids2_all = set()
        
        # Второе объединение: по улице и дому
        street_house_cols = [col for col in ['Улица_clean', 'Дом_clean'] 
                             if col in df1.columns and col in df2.columns]
        
        if len(street_house_cols) > 0:
            merged_street_house, ids1_street_house, ids2_street_house = merge_and_track(
                df1_remaining, df2_remaining, street_house_cols, 'по_улице_и_дому')
            merge_results.append(merged_street_house)
            
            # Обновление оставшихся записей
            df1_remaining = df1_remaining[~df1_remaining['id_1'].isin(ids1_street_house)]
            df2_remaining = df2_remaining[~df2_remaining['id_2'].isin(ids2_street_house)]
        else:
            ids1_street_house = set()
            ids2_street_house = set()
        
        # Третье объединение: по улице, дому и корпусу (если применимо)
        street_house_corpus_cols = [col for col in ['Улица_clean', 'Дом_clean', 'Корпус_clean'] 
                                   if col in df1.columns and col in df2.columns]
        
        if len(street_house_corpus_cols) > 2:  # Убедимся, что есть Корпус
            merged_corpus, ids1_corpus, ids2_corpus = merge_and_track(
                df1_remaining, df2_remaining, street_house_corpus_cols, 'по_улице_дому_корпусу')
            merge_results.append(merged_corpus)
            
            # Обновление оставшихся записей
            df1_remaining = df1_remaining[~df1_remaining['id_1'].isin(ids1_corpus)]
            df2_remaining = df2_remaining[~df2_remaining['id_2'].isin(ids2_corpus)]
        else:
            ids1_corpus = set()
            ids2_corpus = set()
        
        # Четвертое объединение: по улице, дому и литере (если применимо)
        street_house_liter_cols = [col for col in ['Улица_clean', 'Дом_clean', 'Литера_clean'] 
                                  if col in df1.columns and col in df2.columns]
        
        if len(street_house_liter_cols) > 2:  # Убедимся, что есть Литера
            merged_liter, ids1_liter, ids2_liter = merge_and_track(
                df1_remaining, df2_remaining, street_house_liter_cols, 'по_улице_дому_литере')
            merge_results.append(merged_liter)
            
            # Обновление оставшихся записей
            df1_remaining = df1_remaining[~df1_remaining['id_1'].isin(ids1_liter)]
            df2_remaining = df2_remaining[~df2_remaining['id_2'].isin(ids2_liter)]
        else:
            ids1_liter = set()
            ids2_liter = set()
        
        # Шаг 3: Подготовка необъединенных записей
        # Функция для подготовки необъединенных записей
        def prepare_unmerged_df(df, other_df, id_col, source_name):
            if df.empty:
                return pd.DataFrame()
            
            # Добавляем суффикс к колонкам
            suffix = '_citywalls' if source_name == 'только_citywalls' else '_opendata'
            rename_cols = {col: col + suffix for col in df.columns 
                          if col not in [id_col, 'Улица_clean', 'Дом_clean', 'Корпус_clean', 'Литера_clean']
                          and not col.endswith(('_citywalls', '_opendata'))}
            df = df.rename(columns=rename_cols)
            
            # Добавляем пустые колонки из другого датафрейма
            other_suffix = '_opendata' if suffix == '_citywalls' else '_citywalls'
            for col in other_df.columns:
                if col.endswith(other_suffix) and col not in df.columns:
                    df[col] = None
            
            df['merge_type'] = source_name
            return df
        
        # Подготовка оставшихся записей
        df1_only = prepare_unmerged_df(df1_remaining, df2, 'id_1', 'только_citywalls')
        df2_only = prepare_unmerged_df(df2_remaining, df1, 'id_2', 'только_opendata')
        
        # Добавляем необъединенные записи к результатам
        if not df1_only.empty:
            merge_results.append(df1_only)
        if not df2_only.empty:
            merge_results.append(df2_only)
        
        # Шаг 4: Объединение всех результатов
        if merge_results:
            final_merged = pd.concat(merge_results, ignore_index=True)
        else:
            return pd.DataFrame()  # Пустой датафрейм если нечего объединять
        
        # Шаг 5: Очистка и итоговая обработка
        # Создаем единые колонки для адресных данных
        for col in ['Улица', 'Дом', 'Корпус', 'Литера']:
            citywalls_col = f'{col}_citywalls'
            opendata_col = f'{col}_opendata'
            
            if citywalls_col in final_merged.columns and opendata_col in final_merged.columns:
                final_merged[col] = final_merged[citywalls_col].fillna(final_merged[opendata_col])
            elif citywalls_col in final_merged.columns:
                final_merged[col] = final_merged[citywalls_col]
            elif opendata_col in final_merged.columns:
                final_merged[col] = final_merged[opendata_col]
        
        # Создаем нормализованный адрес
        final_merged['normalized_address'] = final_merged.apply(
            lambda row: f"{row.get('Улица', '')}, {row.get('Дом', '')}{' лит.' + row.get('Литера', '') if row.get('Литера', '') else ''}{' корп.' + row.get('Корпус', '') if row.get('Корпус', '') else ''}".strip(),
            axis=1
        )
        
        # Удаляем служебные колонки
        cols_to_drop = ['id_1', 'id_2', 'Улица_clean', 'Дом_clean', 'Корпус_clean', 'Литера_clean']
        final_merged = final_merged.drop([col for col in cols_to_drop if col in final_merged.columns], axis=1)
        
        # Выводим статистику объединения
        stats = {
            'total': len(final_merged),
            'by_all': len(ids1_all),
            'by_street_house': len(ids1_street_house),
            'by_corpus': len(ids1_corpus),
            'by_liter': len(ids1_liter),
            'citywalls_only': len(df1_only) if not df1_only.empty else 0,
            'opendata_only': len(df2_only) if not df2_only.empty else 0
        }
        
        logger.info(f"Всего записей после объединения: {stats['total']}")
        logger.info(f"Объединено по всем полям: {stats['by_all']}")
        logger.info(f"Объединено по улице и дому: {stats['by_street_house']}")
        logger.info(f"Объединено по улице, дому и корпусу: {stats['by_corpus']}")
        logger.info(f"Объединено по улице, дому и литере: {stats['by_liter']}")
        logger.info(f"Только из citywalls: {stats['citywalls_only']}")
        logger.info(f"Только из opendata: {stats['opendata_only']}")
        
        return final_merged
    
    # Применяем функцию умного объединения
    combined_df = merge_address_datasets(citywalls_prepared, opendata_prepared)
    
    # Если нужно, создаем JSON-представление данных citywalls
    if not combined_df.empty:
        try:
            # Выбираем колонки из citywalls
            citywalls_cols = [col for col in combined_df.columns if col.endswith('_citywalls') 
                            or col in ['Название', 'Фото', 'Архитекторы', 'Год постройки', 'Стиль', 'Комментарии', 'Ссылка']]
            
            if citywalls_cols:
                import json
                combined_df['citywalls_data'] = combined_df[citywalls_cols].apply(
                    lambda x: json.dumps(x.dropna().to_dict(), ensure_ascii=False) 
                            if not x.dropna().empty else None, 
                    axis=1
                )
        except Exception as e:
            logger.error(f"Ошибка при создании JSON-представления: {e}")
    
    # Сохраняем результат
    output_file = data_dir["output"] / "combined_buildings_data.xlsx"
    combined_df.to_excel(output_file, index=False)
    logger.info(f"Объединенные данные сохранены в файл {output_file}")
    
    return combined_df
