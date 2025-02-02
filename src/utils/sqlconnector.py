import sqlalchemy as sa
from dataclasses import dataclass
import pandas as pd

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
            
            
    def update_data(self, table_name: str, data: list[dict], condition: str, column_to_update:str) -> int:
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
                update_value = {column_to_update:row[column_to_update]}
                
                # Construccion query
                query = table.update().values(update_value)
                query = query.where(table.c[condition] == row[condition])
            
                # Ejecutar query
                result = conn.execute(query)
                rows_updated += result.rowcount  # Incrementar cuenta

        return rows_updated
    
    def read_data(self, table_name: str, conditions: dict = None, index_col: str = None) -> pd.DataFrame:
        """
        Lee la tabla indicada de la base de datos.
        
        Args:
            table_name (str): Nombre de la tabla desde la que se leerán los datos.
            conditions (dict, optional): Diccionario de pares columna-valor para filtrar filas (opcional).
            index_col (str, optional): Columna que se usará como índice del DataFrame (opcional).

        Returns:
            pandas.DataFrame: DataFrame de Pandas que contiene los datos filtrados.
        """

        table = self._get_metadata(table_name)

        # Build the query
        query = table.select()
        if conditions:
            for column, value in conditions.items():
                query = query.where(table.c[column] == value)

        # Execute the query and fetch the results
        with self.connection.connect() as conn:
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Set the specified column as the index
        if index_col:
            if index_col in df.columns:
                df.set_index(index_col, inplace=True)
            else:
                raise ValueError(f"Columna '{index_col}' no existe en '{table_name}'.")

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
        

