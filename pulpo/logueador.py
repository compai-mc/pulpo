# logger.py
import logging

logging.basicConfig(
    level=logging.INFO,
    format="📌 %(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s(): /n %(message)s"
)


logger = logging.getLogger("mi_app")

def set_log_level(level_name: str):
    """Cambia el nivel de logging en tiempo de ejecución."""
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Nivel de log no válido: {level_name}")
    logger.setLevel(level)
    logger.info(f"Nivel de log cambiado a {level_name.upper()}")