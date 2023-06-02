from sqlalchemy import create_engine
import os

class Storedata:
    def __init__(self):
        self.host = None
        self.port = None
        self.database = None
        self.user = None
        self.password = None
        self.connection = None
        self.database_type = None

    def create_connection(self,database_type):
        self.database_type = database_type
        if database_type.lower() == 'postgres':
            return self._create_postgres_connection()
        else:
            raise ValueError('Invalid database type')

    def _create_postgres_connection(self):
        # Set your PostgreSQL connection parameters
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        self.engine = None
        # Establish a connection to the PostgreSQL database
        try:
            self.engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}')
        except Exception as e:
            print(f'Error creating postgres connection with engine: {self.engine}:{e}')
        return self.engine

    def write_file_to_path(self,df,filename):
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                print(f"Directory '{directory}' created.")
            except Exception as e:
                print(f"Error occured while creating directory '{directory}'.")
        
        #TO remove any existing files in the parquet directory
        if os.path.exists(filename):
                os.system(f"rm -r {filename}")
        #Write data to the directory
        df.to_parquet(filename)
        
    
    def write_data_to_db(self,df):
        if self.engine is None:
            raise Exception("Database connection error. Please check your connection settings and set them by calling create_connection")
        
        if self.database_type == 'postgres':
            self._write_data_to_postgres(df)
    
    def _write_data_to_postgres(self,df):
        try:
            df.to_sql(name='ny_taxi_data', con=self.engine, if_exists='append', index=False)
        except Exception as e:
            print("An error occurred while writing data to db:", str(e))




    
