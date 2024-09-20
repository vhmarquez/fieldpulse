from dotenv import dotenv_values
import sqlalchemy as sa

# ===================================
# Load Environment Variables
# ===================================

config = dotenv_values(".env")
mssql_user = config['MSSQL_USER']
mssql_pw = config['MSSQL_PW']
mssql_server_name = config['MSSQL_SERVER_NAME']
mssql_server_port = config['MSSQL_SERVER_PORT']
mssql_database_name = config['MSSQL_DATABASE_NAME']
mssql_driver = config['MSSQL_DRIVER']

# ===================================
# Establish Database Class
# ===================================

class FieldSurgeDatabase():

    def __init__(self):

        self.connection_string = f'driver={mssql_driver};server={mssql_server_name};port={mssql_server_port};database={mssql_database_name};uid={mssql_user};pwd={mssql_pw};TrustServerCertificate=yes'
        self.connection_url = sa.URL.create('mssql+pyodbc', query={'odbc_connect': self.connection_string})

    def connect(self):

        engine = sa.create_engine(self.connection_url, echo=True, pool_reset_on_return=None)

        return engine
    
