from clickhouse_driver import Client
import pandas as pd

class ClickHouseHandler:
    def __init__(self, host, port, database, user, jwt_token=None):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.jwt_token = jwt_token
        self.client = None
        
    def connect(self):
        """Establish connection to ClickHouse database"""
        try:
            # Use JWT token if provided
            if self.jwt_token:
                self.client = Client(
                    host=self.host,
                    port=int(self.port),
                    database=self.database,
                    user=self.user,
                    token=self.jwt_token
                )
            else:
                # Connect without JWT token (likely for testing)
                self.client = Client(
                    host=self.host,
                    port=int(self.port),
                    database=self.database,
                    user=self.user
                )
            
            # Test connection with a simple query
            self.client.execute('SELECT 1')
            return True, "Connection successful"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def get_tables(self):
        """Get list of tables in the database"""
        try:
            query = f"SHOW TABLES FROM {self.database}"
            result = self.client.execute(query)
            tables = [table[0] for table in result]
            return True, tables
        except Exception as e:
            return False, f"Failed to get tables: {str(e)}"
    
    def get_table_schema(self, table_name):
        """Get schema of a specific table"""
        try:
            query = f"DESCRIBE TABLE {self.database}.{table_name}"
            result = self.client.execute(query)
            columns = [{"name": col[0], "type": col[1]} for col in result]
            return True, columns
        except Exception as e:
            return False, f"Failed to get schema: {str(e)}"
    
    def execute_query(self, query):
        """Execute a custom query"""
        try:
            result = self.client.execute(query)
            return True, result
        except Exception as e:
            return False, f"Query execution failed: {str(e)}"
    
    def preview_data(self, table_name, columns=None, limit=100):
        """Preview data from a table with selected columns"""
        try:
            cols = "*" if not columns else ", ".join(columns)
            query = f"SELECT {cols} FROM {self.database}.{table_name} LIMIT {limit}"
            result = self.client.execute(query, with_column_types=True)
            
            # Extract data and column info
            data = result[0]
            column_names = [col[0] for col in result[1]]
            
            # Convert to pandas DataFrame for easier handling
            df = pd.DataFrame(data, columns=column_names)
            return True, {"data": df.to_dict('records'), "columns": column_names}
        except Exception as e:
            return False, f"Preview failed: {str(e)}"
    
    def export_data(self, table_name, columns=None, join_tables=None, join_conditions=None):
        """Export data from ClickHouse table(s) with selected columns"""
        try:
            cols = "*" if not columns else ", ".join(columns)
            
            # Basic query for single table
            query = f"SELECT {cols} FROM {self.database}.{table_name}"
            
            # Add JOIN if specified
            if join_tables and join_conditions:
                for i, join_table in enumerate(join_tables):
                    query += f" JOIN {self.database}.{join_table} ON {join_conditions[i]}"
            
            # Execute query and fetch data
            result = self.client.execute(query, with_column_types=True)
            
            # Extract data and column info
            data = result[0]
            column_names = [col[0] for col in result[1]]
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(data, columns=column_names)
            
            return True, {"data": df, "count": len(df)}
        except Exception as e:
            return False, f"Export failed: {str(e)}"
    
    def import_data(self, data_df, target_table, columns=None):
        """Import data into ClickHouse table"""
        try:
            # If columns specified, use only those columns
            if columns:
                data_df = data_df[columns]
            
            # Get the data as list of tuples for insertion
            data = [tuple(x) for x in data_df.to_numpy()]
            
            # Create table if it doesn't exist
            if target_table not in self.get_tables()[1]:
                # Generate CREATE TABLE statement based on DataFrame dtypes
                create_table_query = self._generate_create_table_query(data_df, target_table)
                self.client.execute(create_table_query)
            
            # Insert data into table
            column_names = ", ".join(data_df.columns)
            self.client.execute(
                f"INSERT INTO {self.database}.{target_table} ({column_names}) VALUES",
                data
            )
            
            return True, {"count": len(data_df)}
        except Exception as e:
            return False, f"Import failed: {str(e)}"
    
    def _generate_create_table_query(self, df, table_name):
        """Generate CREATE TABLE query based on DataFrame structure"""
        # Map pandas dtypes to ClickHouse types
        type_mapping = {
            'int64': 'Int64',
            'float64': 'Float64',
            'object': 'String',
            'bool': 'UInt8',
            'datetime64[ns]': 'DateTime',
            'category': 'String',
        }
        
        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            ch_type = type_mapping.get(dtype, 'String')
            columns.append(f"`{col}` {ch_type}")
        
        create_query = f"""
        CREATE TABLE {self.database}.{table_name} (
            {', '.join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """
        
        return create_query
