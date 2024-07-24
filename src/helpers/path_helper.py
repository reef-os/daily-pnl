import os


class PathHelper:
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'local')  # local varsayılan olarak ayarlanır.
        self.base_paths = {
            'local': {
                'bronze': 'data/bronze',
                'silver': 'data/silver',
                'gold': 'data/gold'
            },
            'aws': {
                'bronze': 's3://bucket-name/bronze',
                'silver': 's3://bucket-name/silver',
                'gold': 's3://bucket-name/gold'
            }
        }

    def get_path(self, tier: str) -> str:
        if tier not in self.base_paths[self.environment]:
            raise ValueError(
                f"Unknown tier: {tier}. Valid tiers are: {', '.join(self.base_paths[self.environment].keys())}")
        return self.base_paths[self.environment][tier]


# Kullanım örneği
path_helper = PathHelper()
bronze_path = path_helper.get_path('bronze')
silver_path = path_helper.get_path('silver')
gold_path = path_helper.get_path('gold')

print(f"Bronze Path: {bronze_path}")
print(f"Silver Path: {silver_path}")
print(f"Gold Path: {gold_path}")

"""
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# PathHelper sınıfını çağır
path_helper = PathHelper()
bronze_path = path_helper.get_path('bronze')

print(f"Bronze Path: {bronze_path}")

"""