# Gunicorn config: filtra logs irrelevantes (keepalive, scanners de bot)
# pra que /admin mostre so eventos reais do time.
import logging


class FiltroNoise(logging.Filter):
    """Esconde access logs de endpoints de health/scanner pra reduzir noise."""
    SILENCIAR = (
        '/keepalive',
        '/apple-touch-icon',
        '/favicon.ico',
        '/robots.txt',
    )

    def filter(self, record):
        msg = record.getMessage()
        for path in self.SILENCIAR:
            if path in msg:
                return False
        return True


def post_fork(server, worker):
    # Aplica filtro em todos os loggers de access
    for name in ('gunicorn.access', 'gunicorn.error'):
        logger = logging.getLogger(name)
        logger.addFilter(FiltroNoise())


# Gunicorn config base
bind = "0.0.0.0:" + __import__('os').environ.get('PORT', '10000')
workers = 2
timeout = 300
accesslog = "-"
errorlog = "-"
capture_output = True
loglevel = "info"
