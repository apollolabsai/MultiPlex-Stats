"""
Flask application configuration.
"""
import os


class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-please-change-in-production'
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'instance', 'multiplex_stats.db'))
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'connect_args': {'timeout': 30},
    }
    STADIA_MAPS_API_KEY = (os.environ.get('STADIA_MAPS_API_KEY') or '').strip()
    MDBLIST_API_KEY = (os.environ.get('MDBLIST_API_KEY') or '').strip()
    STADIA_MAP_STYLE = 'alidade_smooth_dark'
    STADIA_MAP_TILE_URL = (
        os.environ.get('STADIA_MAP_TILE_URL')
        or f"https://tiles.stadiamaps.com/tiles/{STADIA_MAP_STYLE}/{{z}}/{{x}}/{{y}}{{r}}.png"
    )
    STADIA_MAP_ATTRIBUTION = (
        '&copy; <a href="https://stadiamaps.com/" target="_blank" rel="noopener">Stadia Maps</a> '
        '&copy; <a href="https://openmaptiles.org/" target="_blank" rel="noopener">OpenMapTiles</a> '
        '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a>'
    )
    STADIA_MAP_MAX_ZOOM = int(os.environ.get('STADIA_MAP_MAX_ZOOM', '20'))


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
