import os


class Config:
    ip: str = os.environ.get('ip_db')
    port: int = os.environ.get('port_db')
