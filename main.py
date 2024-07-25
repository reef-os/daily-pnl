from src.etl_manager import ETLManager


if __name__ == "__main__":
    etl_manager = ETLManager('2024-04-01', '2024-04-30')
    etl_manager.start()
