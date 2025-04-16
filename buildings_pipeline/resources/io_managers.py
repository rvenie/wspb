from dagster import IOManager, io_manager, Field, MetadataValue
import pandas as pd

class DataFrameIOManager(IOManager):
    def __init__(self, db_resource=None):
        self.db_resource = db_resource
    
    def handle_output(self, context, obj):
        if isinstance(obj, pd.DataFrame):
            # Записываем метаданные
            context.add_output_metadata({
                "row_count": len(obj),
                "column_count": len(obj.columns),
                "columns": MetadataValue.json(list(obj.columns)),
            })
            
            # Сохраняем в базу данных, если ресурс доступен
            if self.db_resource and context.metadata.get("save_to_db", True):
                table_name = context.metadata.get("table_name", context.name)
                schema = context.metadata.get("schema", "buildings")
                if_exists = context.metadata.get("if_exists", "replace")
                
                try:
                    self.db_resource.insert_dataframe(
                        obj, 
                        table_name=table_name,
                        schema=schema,
                        if_exists=if_exists
                    )
                    context.log.info(f"DataFrame сохранен в {schema}.{table_name}")
                except Exception as e:
                    context.log.error(f"Ошибка сохранения DataFrame в базу данных: {e}")
    
    def load_input(self, context):
        if context.upstream_output is None:
            return pd.DataFrame()
        return context.upstream_output.value

@io_manager(
    config_schema={"use_db": Field(bool, is_required=False, default_value=True)}
)
def dataframe_io_manager(init_context):
    use_db = init_context.resource_config.get("use_db", True)
    db_resource = init_context.resources.postgres if use_db else None
    return DataFrameIOManager(db_resource)
