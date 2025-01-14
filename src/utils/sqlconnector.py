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
        # Construct the connection URL
        self.url = f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        # Create the SQLAlchemy engine
        self.connection = sa.create_engine(self.url, echo=True)
        # Initialize metadata
        self.metadata = sa.MetaData(bind=self.connection)
        self.metadata.reflect()  # Reflects existing tables in the database
    
    def insert_data(self, table_name: str, data: list[dict]):
        """
        Inserts data into the specified table, ignoring duplicates using INSERT IGNORE.

        :param table_name: Name of the table.
        :param data: List of dictionaries representing rows to insert.
        """
        if table_name not in self.metadata.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database.")
        
        table = self.metadata.tables[table_name]
        
        with self.connection.connect() as conn:
            # Create the insert statement
            stmt = insert(table).values(data)
            
            # Use prefix_with to add the 'IGNORE' keyword for MySQL
            stmt = stmt.prefix_with('IGNORE')
            
            # Execute the query with the data
            conn.execute(stmt)
    
    def read_data(self, table_name: str, conditions: dict = None, index_col: str = None) -> pd.DataFrame:
        """
        Reads data from the specified table with optional conditions and sets the index of the DataFrame.

        :param table_name: Name of the table.
        :param conditions: Dictionary of column-value pairs for filtering rows (optional).
        :param index_col: Column to use as the index of the DataFrame (optional).
        :return: Pandas DataFrame containing the filtered data.
        """
        if table_name not in self.metadata.tables:
            raise ValueError(f"Table '{table_name}' does not exist in the database.")
        
        table = self.metadata.tables[table_name]

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
                raise ValueError(f"Column '{index_col}' does not exist in the table '{table_name}'.")

        return df

