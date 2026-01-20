"""
Data models and configuration classes for Tautulli analytics.
"""

from dataclasses import dataclass


@dataclass
class ServerConfig:
    """Configuration for a Tautulli server instance."""

    name: str
    ip_address: str
    api_key: str
    use_ssl: bool = False
    verify_ssl: bool = False

    @property
    def base_url(self) -> str:
        """Get the base URL for API requests."""
        protocol = "https" if self.use_ssl else "http"
        return f"{protocol}://{self.ip_address}/api/v2"

    def __repr__(self) -> str:
        """String representation with masked API key."""
        masked_key = f"{self.api_key[:4]}...{self.api_key[-4:]}" if len(self.api_key) > 8 else "***"
        return f"ServerConfig(name='{self.name}', ip_address='{self.ip_address}', api_key='{masked_key}')"


@dataclass
class MediaColors:
    """Color configuration for visualizations."""

    server_a_tv: str = '#E6B413'
    server_a_movies: str = '#FFE548'
    server_b_tv: str = '#e36414'
    server_b_movies: str = '#f18a3d'

    def get_color_map(self, server_a_name: str, server_b_name: str | None) -> dict[str, str]:
        """Get color mapping dictionary for one or two servers."""
        color_map = {
            f'{server_a_name}_TV': self.server_a_tv,
            f'{server_a_name}_Movies': self.server_a_movies,
        }

        if server_b_name:
            color_map.update({
                f'{server_b_name}_TV': self.server_b_tv,
                f'{server_b_name}_Movies': self.server_b_movies,
            })

        return color_map
