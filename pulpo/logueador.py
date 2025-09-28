# logueador.py
import logging


class Logueador:
    def __init__(self, nombre="mi_app", fichero="app/log/app.log", nivel="INFO"):
        self.logger = logging.getLogger(nombre)
        self._configurar(fichero, nivel)

    def _configurar(self, fichero, nivel):
        """Configura el logger con fichero y nivel inicial."""
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        nivel_int = getattr(logging, nivel.upper(), logging.INFO)

        handler = logging.FileHandler(fichero, mode="a", encoding="utf-8")
        formatter = logging.Formatter(
            "📌 %(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(funcName)s():\n %(message)s"
        )
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)
        self.logger.setLevel(nivel_int)
        self.logger.propagate = False

        self.fichero = fichero
        

    def set_log_level(self, level_name: str):
        """Cambia el nivel de logging en tiempo de ejecución."""
        level = getattr(logging, level_name.upper(), None)
        if not isinstance(level, int):
            raise ValueError(f"Nivel de log no válido: {level_name}")
        self.logger.setLevel(level)
        self.logger.info(f"Nivel de log cambiado a {level_name.upper()}")

    def set_log_file(self, fichero: str):
        """Cambia el fichero de log en tiempo de ejecución."""
        current_level = logging.getLevelName(self.logger.level)
        self._configurar(fichero, current_level)
        self.logger.info(f"Cambiado el fichero de log a {fichero}")


    # Métodos de conveniencia para usar log.debug(), log.info(), etc.
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, stacklevel=2, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, stacklevel=2, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, stacklevel=2, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, stacklevel=2, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, stacklevel=2, **kwargs)


# Ejemplo de uso
if __name__ == "__main__":
    log = Logueador(fichero="app/log/xxx.log", nivel="INFO")

    def prueba():
        log.info("Mensaje inicial 🙂")

    prueba()


    log.set_log_level("DEBUG")
    log.debug("Ahora en DEBUG")

    #log.set_log_file("log/otro.log")
    #log.info("Este mensaje va al nuevo fichero")
