import sqlalchemy as sa
from sqlalchemy import and_
from dataclasses import dataclass
import pandas as pd
import math

@dataclass
class SQLConnector:
    dialect:str = 'mysql'
    driver:str = 'pymysql'
    host:str = '192.168.1.51'
    port:str = '3306'
    database:str = 'market_predictor'
    username:str = ''
    password:str = '' 
    
    def __post_init__(self):
        # Construye url
        self.url = f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        # SQLAlchemy engine
        self.connection = sa.create_engine(self.url, echo=True)
        # Inicializar metadatos
        self.metadata = sa.MetaData(bind=self.connection)
        self.metadata.reflect()  # Reflects existing tables in the database
        
    def _get_metadata(self, table_name:str):
        if table_name not in self.metadata.tables:
            raise ValueError(f"La tabla '{table_name}' no existe en la base de datos.")
        
        table = self.metadata.tables[table_name]
        
        return table
    
    def insert_data(self, table_name: str, data: list[dict], prefix=''):
        """
        Args:
            table_name (str): Nombre de la tabla en la que se insertarán los datos.
            data (list[dict]): Lista de diccionarios que representan las filas a insertar.
            prefix (str, optional): Prefijo para la query. IGNORE para ignorar duplicados.

        Returns:
            None
        """

        table = self._get_metadata(table_name)
        
        with self.connection.connect() as conn:
            # Create the insert statement
            stmt = sa.insert(table).values(data)
            
            # Use prefix_with to add the 'IGNORE' keyword for MySQL
            if 'DUPLICATE' in prefix:
                stmt = stmt.on_duplicate_key_update(
                    data=stmt.inserted.data,
                    status='U'
                )
            else:
                stmt = stmt.prefix_with(prefix)
            
            # Execute the query with the data
            conn.execute(stmt)
            
            
    def update_data(self, table_name: str, data: list[dict], condition: str, columns_to_update:list[str]) -> int:
        """
        Actualiza la tabla proporcionada de la base de datos usando las condiciones
        definidas en conditions.

        Args:
            table_name (str): Nombre de la tabla
            data (list[dict]): Lista de diccionarios con los valores a actualizar
            conditions (dict): Condiciones a aplicar en clausula WHERE
        
        Returns:
            rows_updated (int): Numero de filas actualizadas
        """
        table = self._get_metadata(table_name)
        
        rows_updated = 0
        
        # Conexiion y ejecucionn query
        with self.connection.connect() as conn:
            for row in data:
                update_values = {
                    col: (None if isinstance(row[col], float) and math.isnan(row[col]) else row[col])
                    for col in columns_to_update
                }
                
                # Construcción de la consulta
                query = table.update().where(table.c[condition] == row[condition]).values(update_values)
                
                # Ejecutar la consulta y actualizar el contador
                rows_updated += conn.execute(query).rowcount

        return rows_updated
    
    from sqlalchemy import and_

    def read_data(self, table_name: str, conditions: dict = None, index_col: str = None) -> pd.DataFrame:
        """
        Reads data from the specified table in the database.

        Args:
            table_name (str): Name of the table to read data from.
            conditions (dict, optional): Dictionary with column conditions. 
                                        Supports tuples for operations, e.g., {"col": (">", value)}.
            index_col (str, optional): Column to be used as DataFrame index.

        Returns:
            pandas.DataFrame: DataFrame with the filtered data.
        """

        table = self._get_metadata(table_name)

        # Build the query
        query = table.select()
        if conditions:
            filters = []
            for column, condition in conditions.items():
                if isinstance(condition, tuple) and len(condition) == 2:
                    op, value = condition
                    if op == "=":
                        filters.append(table.c[column] == value)
                    elif op == ">":
                        filters.append(table.c[column] > value)
                    elif op == "<":
                        filters.append(table.c[column] < value)
                    elif op == ">=":
                        filters.append(table.c[column] >= value)
                    elif op == "<=":
                        filters.append(table.c[column] <= value)
                    elif op == "!=":
                        filters.append(table.c[column] != value)
                    else:
                        raise ValueError(f"Operacion no soportada: {op}")
                else:  # Default to '=' if no operator is specified
                    filters.append(table.c[column] == condition)

            query = query.where(and_(*filters))

        # Execute the query and fetch results
        with self.connection.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Set the index if needed
        if index_col:
            if index_col in df.columns:
                df.set_index(index_col, inplace=True)
            else:
                raise ValueError(f"Columna '{index_col}' no existe '{table_name}'.")

        return df

    
    def custom_query(self, query:str) -> pd.DataFrame:
        """
        Ejecuta unna query customizada en lenguaje MySQL.

        Args:
            query (str): Query a ejecutar. Se connvierte a formato text.

        Raises:
            ValueError: Error en caso de query en formato differente a Str

        Returns:
            DataFrame: dataframe con los datos requeridos
        """
        
        custom_query = sa.text(query)
        # Execute the query and fetch the results
        with self.connection.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        return df
        

