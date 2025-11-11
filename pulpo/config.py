from pathlib import Path

from dotenv import load_dotenv

print(f"Antes de carga de configuración de.env")
env_path = Path("/home/pepe/Desarrollo/compai/deploy/desarrollo/desarrollo-compai/.env")
print(f"Cargando configuracion ")
if env_path.exists():
    print(f"Cargando configuración de {env_path} ({env_path.stat().st_size} bytes)")
    load_dotenv(env_path)
else:
    print(f"Ya ha sido inyectado previamente el .env en el entorno desde el compose")

