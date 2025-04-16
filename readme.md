Текущий прогресс по ПЗ указан в соответствующих Jupyter-тетрадках.

```
buildings_pipeline/
├── buildings_pipeline/
│   ├── __init__.py
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── citywalls.py
│   │   ├── spb_open_data.py
│   │   └── combined_data.py
│   ├── resources/
│   │   ├── __init__.py
│   │   └── resources.py
│   └── definitions.py
├── setup.py
└── pyproject.toml
```

Планы:

- Обогащение данных:
  Добавить геокоординаты для географического анализа
  Собрать информацию о реставрациях и текущем состоянии зданий
  Включить данные о культурной значимости (памятники, охраняемые объекты)

- Анализ изображений:
  Извлечение визуальных признаков из фотографий зданий
  Классификация архитектурных элементов (колонны, арки, фронтоны)
