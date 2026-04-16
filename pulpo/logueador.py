import logging
import inspect


class Logueador:
    def __init__(self, nombre=__name__):
        self.logger = logging.getLogger(nombre)
        self.formatter = None

    def set_propagate(self, propagate: bool):
        self.logger.propagate = propagate

    def configurar(self, fichero, nivel):
        """Configura el logger con fichero y nivel inicial."""

        # Limpia el root logger para evitar duplicación por consola
        logging.getLogger().handlers.clear()

        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        nivel_int = getattr(logging, nivel.upper(), logging.INFO)

        handler = logging.FileHandler(fichero, mode="a", encoding="utf-8")

        formatter = logging.Formatter(
            "📌 %(asctime)s  "
            "%(levelname)-7s  "
            "%(filename)-25s  "
            "%(lineno)-4d  "
            "%(funcName)-20s  "
            "%(message)s"
        )

        handler.setFormatter(formatter)

        self.logger.addHandler(handler)
        self.logger.setLevel(nivel_int)

        self.fichero = fichero
        self.formatter = formatter  # 👈 guardar para reutilizar en print

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
        self.configurar(fichero, current_level)
        self.logger.info(f"Cambiado el fichero de log a {fichero}")

    # =========================
    # PRINT CON FORMATO
    # =========================
    def _print_like_logger(self, level, msg, args):
        if not self.formatter:
            print(msg)
            return

        # Obtener caller real
        frame = inspect.stack()[3]

        record = self.logger.makeRecord(
            name=self.logger.name,
            level=level,
            fn=frame.filename,
            lno=frame.lineno,
            msg=msg,
            args=args,
            exc_info=None
        )

        formatted = self.formatter.format(record)
        print(formatted)

    # =========================
    # MÉTODOS DE LOG
    # =========================
    def debug(self, msg, *args, **kwargs):
        print_console = kwargs.pop("print_console", False)
        self.logger.debug(msg, *args, stacklevel=2, **kwargs)
        if print_console:
            self._print_like_logger(logging.DEBUG, msg, args)

    def info(self, msg, *args, **kwargs):
        print_console = kwargs.pop("print_console", False)
        self.logger.info(msg, *args, stacklevel=2, **kwargs)
        if print_console:
            self._print_like_logger(logging.INFO, msg, args)

    def warning(self, msg, *args, **kwargs):
        print_console = kwargs.pop("print_console", False)
        self.logger.warning(msg, *args, stacklevel=2, **kwargs)
        if print_console:
            self._print_like_logger(logging.WARNING, msg, args)

    def error(self, msg, *args, **kwargs):
        print_console = kwargs.pop("print_console", False)
        self.logger.error(msg, *args, stacklevel=2, **kwargs)
        if print_console:
            self._print_like_logger(logging.ERROR, msg, args)

    def critical(self, msg, *args, **kwargs):
        print_console = kwargs.pop("print_console", False)
        self.logger.critical(msg, *args, stacklevel=2, **kwargs)
        if print_console:
            self._print_like_logger(logging.CRITICAL, msg, args)


# =========================
# INSTANCIA GLOBAL
# =========================
log = Logueador()


# =========================
# EJEMPLO DE USO
# =========================
if __name__ == "__main__":
    log = Logueador("MiApp")
    log.configurar("app/log/xxx.log", "INFO")

    def prueba():
        log.info("Mensaje inicial 🙂", print_console=True)

    prueba()

    log.set_log_level("DEBUG")
    log.debug("Ahora en DEBUG", print_console=True)

    # log.set_log_file("app/log/otro.log")
    # log.info("Este mensaje va al nuevo fichero", print_console=True)