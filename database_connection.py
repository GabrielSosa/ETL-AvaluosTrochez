import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

class DatabaseConnection:
    """
    Clase para manejar la conexión a la base de datos PostgreSQL
    """
    
    def __init__(self):
        # Cargar variables de entorno desde el archivo .env
        load_dotenv()
        
        # Obtener credenciales desde variables de entorno
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')
        
        # Crear el engine de conexión
        self.engine = None
        self.session = None
        self._create_engine()
    
    def _create_engine(self):
        """
        Crear el engine de SQLAlchemy con configuración SSL
        """
        try:
            connection_string = f'postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'
            
            self.engine = create_engine(
                connection_string,
                connect_args={
                    "sslmode": "require",
                    "connect_timeout": 30,  # Timeout de conexión de 30 segundos
                    "application_name": "ETL_Avaluos"  # Identificar la aplicación
                },
                pool_pre_ping=True,  # Verificar conexión antes de usar
                pool_recycle=1800,   # Reciclar conexiones cada 30 minutos
                pool_size=5,         # Tamaño del pool de conexiones
                max_overflow=10,     # Conexiones adicionales permitidas
                echo=False           # No mostrar SQL en logs
            )
            
            # Crear session factory
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            print("✅ Conexión a la base de datos establecida correctamente")
            
        except SQLAlchemyError as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            raise
    
    def get_engine(self):
        """
        Obtener el engine de SQLAlchemy
        """
        return self.engine
    
    def get_session(self):
        """
        Obtener la sesión de SQLAlchemy
        """
        return self.session
    
    def test_connection(self):
        """
        Probar la conexión a la base de datos
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                print("✅ Conexión a la base de datos exitosa")
                return True
        except SQLAlchemyError as e:
            print(f"❌ Error al probar la conexión: {e}")
            return False
    
    def close_connection(self):
        """
        Cerrar la conexión a la base de datos
        """
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
        print("🔒 Conexión a la base de datos cerrada")
    
    def __enter__(self):
        """
        Context manager para usar con 'with'
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Cerrar conexión automáticamente al salir del contexto
        """
        self.close_connection()


# Función de conveniencia para obtener una instancia de conexión
def get_database_connection():
    """
    Obtener una instancia de DatabaseConnection
    """
    return DatabaseConnection()


# Ejemplo de uso
if __name__ == "__main__":
    # Probar la conexión
    db = DatabaseConnection()
    db.test_connection()
    db.close_connection() 