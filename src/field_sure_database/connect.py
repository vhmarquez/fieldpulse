from dotenv import dotenv_values
import sqlalchemy as sa
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
client = SecretClient(vault_url='https://fieldpulsekeyvault.vault.azure.net/', credential=credential)

# ===================================
# Load Environment Variables
# ===================================

mssql_user = client.get('SERVER-USERNAME')
mssql_pw = client.get('SERVER-PW')
mssql_server_name = client.get('SERVER-NAME')
mssql_server_port = client.get('SERVER-PORT')
mssql_database_name = client.get('DATABASE-NAME')
mssql_driver = client.get('SERVER-DRIVER')

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
    
