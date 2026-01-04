# MultiPlex Stats - Docker Deployment

Docker containerization for the MultiPlex Stats Tautulli analytics application.

## Quick Start

### Using Docker Compose (Recommended)

```bash
# Build and start the container
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the container
docker-compose down
```

Access the web interface at: http://localhost:8487

### Using Docker CLI

```bash
# Build the image
docker build -t multiplex-stats .

# Run the container
docker run -d \
  --name multiplex-stats \
  -p 8487:8487 \
  -v $(pwd)/instance:/app/instance \
  multiplex-stats

# View logs
docker logs -f multiplex-stats

# Stop the container
docker stop multiplex-stats
docker rm multiplex-stats
```

## Configuration

### Environment Variables

- `PORT`: Application port (default: 8487)
- `FLASK_ENV`: Flask environment (default: production)

### Data Persistence

The `instance/` directory is volume-mounted to persist:
- SQLite database (`instance/multiplex_stats.db`)
- Chart cache (`instance/cache/`)

## First-Time Setup

1. Start the container
2. Navigate to http://localhost:8487
3. Click "Settings" in the navigation
4. Configure your Tautulli server(s):
   - Server Name
   - IP Address:Port (e.g., 192.168.1.100:8181)
   - API Key
   - Check "Use HTTPS/SSL" if your Tautulli uses HTTPS
   - Leave "Verify SSL Certificate" unchecked for self-signed certificates
5. Click "Save Server"
6. Return to dashboard and click "Run Analytics"

## Updating

```bash
# Pull latest changes
git pull

# Rebuild and restart
docker-compose up -d --build
```

## Port Customization

To use a different port, edit `docker-compose.yml`:

```yaml
ports:
  - "YOUR_PORT:8487"
environment:
  - PORT=8487
```

Note: The internal container port stays 8487, only change the external mapping.

## Troubleshooting

### Container won't start
```bash
# Check logs
docker-compose logs

# Check if port is already in use
lsof -i :8487
```

### Database issues
```bash
# Stop container
docker-compose down

# Remove database (you'll need to reconfigure)
rm -rf instance/multiplex_stats.db

# Restart
docker-compose up -d
```

### SSL Certificate errors
If you see SSL certificate verification errors when connecting to Tautulli:
1. Go to Settings
2. Edit your server configuration
3. Ensure "Use HTTPS/SSL" is checked
4. **Uncheck** "Verify SSL Certificate" if using self-signed certificates
5. Save and try running analytics again

## Building for Distribution

### Build and push to Docker Hub
```bash
# Build for your architecture
docker build -t yourusername/multiplex-stats:latest .

# Push to Docker Hub
docker push yourusername/multiplex-stats:latest
```

### Multi-architecture build
```bash
# Build for multiple platforms
docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 \
  -t yourusername/multiplex-stats:latest \
  --push .
```

## Health Check

The container includes a health check that runs every 30 seconds:
```bash
# Check container health
docker ps
```

Look for "healthy" status in the STATUS column.

## Security Notes

- The application runs on `0.0.0.0` to accept connections from outside the container
- No authentication is built into the web interface
- Use a reverse proxy (nginx, Traefik) with authentication if exposing to the internet
- Tautulli API keys are stored in the SQLite database
- Consider using Docker secrets for production deployments

## Resource Usage

Typical resource consumption:
- **CPU**: Low (spikes during analytics runs)
- **Memory**: ~200-300MB
- **Disk**: Minimal (database + cache typically < 100MB)

## Support

For issues related to:
- **Docker setup**: Check this README and container logs
- **Application features**: See main README.md
- **Tautulli connection**: Verify server settings and network connectivity
