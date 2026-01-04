# Docker Hub Publishing Guide

This guide walks you through setting up automated Docker image publishing to Docker Hub using GitHub Actions.

## Prerequisites

1. **Docker Hub Account**: Create a free account at https://hub.docker.com
2. **GitHub Repository**: Your repository at https://github.com/apollolabsai/MultiPlex-Stats

## Step-by-Step Setup

### 1. Create Docker Hub Repository

1. Log in to Docker Hub (https://hub.docker.com)
2. Click "Create Repository"
3. Configure your repository:
   - **Name**: `multiplex-stats`
   - **Description**: `MultiPlex Stats - Tautulli analytics web interface with beautiful visualizations`
   - **Visibility**: Public (or Private if you prefer)
4. Click "Create"

Your image will be available at: `your-username/multiplex-stats`

### 2. Generate Docker Hub Access Token

1. Go to Docker Hub → Account Settings → Security
2. Click "New Access Token"
3. Configure the token:
   - **Description**: `GitHub Actions - MultiPlex Stats`
   - **Access permissions**: Read & Write
4. Click "Generate"
5. **IMPORTANT**: Copy the token immediately (it won't be shown again!)

### 3. Add Secrets to GitHub Repository

1. Go to your GitHub repository: https://github.com/apollolabsai/MultiPlex-Stats
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** and add:

   **Secret 1:**
   - Name: `DOCKERHUB_USERNAME`
   - Value: Your Docker Hub username

   **Secret 2:**
   - Name: `DOCKERHUB_TOKEN`
   - Value: The access token you generated in step 2

### 4. Merge Docker Branch to Main

The GitHub Actions workflow is configured to trigger on pushes to `main` and `docker` branches, as well as on releases.

```bash
# Switch to main branch
git checkout main

# Merge docker branch
git merge docker

# Push to GitHub
git push origin main
```

This will trigger the first automated build!

## How It Works

### Automatic Triggers

The workflow automatically builds and pushes Docker images when:

1. **Release Created**: When you create a new release on GitHub
   - Tags: `v1.0.0`, `v1.0`, `v1`, `latest`

2. **Push to Main Branch**: When code is pushed to `main`
   - Tags: `latest`, `main-<git-sha>`

3. **Push to Docker Branch**: When code is pushed to `docker`
   - Tags: `docker`, `docker-<git-sha>`

4. **Manual Trigger**: You can manually run the workflow from GitHub Actions tab

### Multi-Architecture Support

The workflow builds images for both:
- **linux/amd64** (Intel/AMD processors)
- **linux/arm64** (Apple Silicon, Raspberry Pi, etc.)

### Version Tagging Strategy

#### For Releases:
When you create a release `v1.2.3`, the following tags are created:
- `1.2.3` - Full version
- `1.2` - Major.minor version
- `1` - Major version only
- `latest` - Always points to the latest release

#### For Branch Pushes:
- `main` - Latest from main branch
- `docker` - Latest from docker branch
- `main-abc123` - Specific commit SHA
- `latest` - Latest stable (from main)

## Publishing Your First Release

### Option 1: Using GitHub Web Interface

1. Go to https://github.com/apollolabsai/MultiPlex-Stats/releases
2. Click **"Draft a new release"**
3. Click **"Choose a tag"** and create new tag: `v1.0.0`
4. Release title: `v1.0.0 - Initial Docker Release`
5. Description:
   ```markdown
   ## 🚀 MultiPlex Stats v1.0.0

   First official Docker release!

   ### Features
   - Web-based configuration interface
   - Multi-server support (up to 2 Tautulli servers)
   - 7 interactive Plotly visualizations
   - HTTPS/SSL support with self-signed certificates
   - Secure non-root container execution
   - Pinned dependencies for stability

   ### Docker Installation
   ```
   docker run -d \
     --name multiplex-stats \
     -p 8487:8487 \
     -v $(pwd)/data:/app/instance \
     your-username/multiplex-stats:latest
   ```

   Or with docker-compose:
   ```yaml
   services:
     multiplex-stats:
       image: your-username/multiplex-stats:latest
       container_name: multiplex-stats
       ports:
         - "8487:8487"
       volumes:
         - ./data:/app/instance
       restart: unless-stopped
   ```
   ```
6. Click **"Publish release"**

The GitHub Action will automatically build and push the Docker image!

### Option 2: Using Git Command Line

```bash
# Create and push a tag
git tag -a v1.0.0 -m "Release v1.0.0 - Initial Docker Release"
git push origin v1.0.0

# Then create the release on GitHub using the tag
```

## Monitoring Build Progress

1. Go to your repository on GitHub
2. Click the **"Actions"** tab
3. You'll see the workflow running
4. Click on the workflow run to see detailed logs
5. Build typically takes 3-5 minutes

## Verifying the Published Image

Once the build completes:

1. Check Docker Hub: https://hub.docker.com/r/your-username/multiplex-stats
2. You should see your image with tags listed
3. Test pulling the image:
   ```bash
   docker pull your-username/multiplex-stats:latest
   ```

## Using the Published Image

### Quick Start

```bash
# Pull and run
docker run -d \
  --name multiplex-stats \
  -p 8487:8487 \
  -v multiplex-data:/app/instance \
  your-username/multiplex-stats:latest

# Access at http://localhost:8487
```

### Using docker-compose.yml

Update your `docker-compose.yml` to use the published image instead of building locally:

```yaml
services:
  multiplex-stats:
    image: your-username/multiplex-stats:latest
    container_name: multiplex-stats
    ports:
      - "8487:8487"
    volumes:
      - ./instance:/app/instance
    environment:
      - PORT=8487
    restart: unless-stopped
```

Then:
```bash
docker-compose pull  # Pull latest image
docker-compose up -d # Start container
```

## Updating the Docker Image

### For Bug Fixes or Minor Updates:

1. Make your code changes
2. Commit and push to `main` branch
3. Create a new release (e.g., `v1.0.1`)
4. GitHub Actions automatically builds and publishes

### For Major Features:

1. Develop on a feature branch
2. Merge to `docker` branch for testing
3. Test the `docker` tagged image
4. Merge to `main` when ready
5. Create a release with new version number

## Version Numbering Guide

Follow [Semantic Versioning](https://semver.org/):

- **MAJOR** version (v2.0.0): Breaking changes, incompatible API changes
- **MINOR** version (v1.1.0): New features, backwards-compatible
- **PATCH** version (v1.0.1): Bug fixes, backwards-compatible

Examples:
- `v1.0.0` - Initial release
- `v1.0.1` - Bug fix for chart rendering
- `v1.1.0` - Add new analytics feature
- `v2.0.0` - Major redesign, database schema changes

## Troubleshooting

### Build Fails

1. Check the Actions logs on GitHub for error messages
2. Common issues:
   - Missing secrets (DOCKERHUB_USERNAME or DOCKERHUB_TOKEN)
   - Invalid Dockerfile syntax
   - Dependency installation failures

### Authentication Fails

1. Verify your Docker Hub credentials in GitHub Secrets
2. Regenerate your Docker Hub access token if needed
3. Ensure the token has Read & Write permissions

### Image Not Appearing on Docker Hub

1. Check that the workflow completed successfully
2. Verify your Docker Hub repository exists
3. Check the workflow logs for push confirmation

## Advanced Usage

### Building Specific Platforms Only

Edit `.github/workflows/docker-publish.yml`:

```yaml
platforms: linux/amd64  # Remove arm64 if not needed
```

### Adding Custom Tags

Edit the `tags` section in the workflow:

```yaml
tags: |
  type=raw,value=stable
  type=raw,value=production
```

### Scheduled Builds

Add a schedule to rebuild periodically:

```yaml
on:
  schedule:
    - cron: '0 0 * * 0'  # Weekly on Sunday at midnight
```

## Security Best Practices

1. ✅ **Never commit tokens**: Always use GitHub Secrets
2. ✅ **Use access tokens**: Don't use your Docker Hub password
3. ✅ **Limit permissions**: Use Read & Write, not Admin
4. ✅ **Rotate tokens**: Regenerate tokens periodically
5. ✅ **Review logs**: Check workflow logs for sensitive data before sharing

## Benefits of This Setup

✅ **Automated**: No manual docker build/push commands
✅ **Consistent**: Same build process every time
✅ **Multi-arch**: Works on Intel, AMD, ARM, and Apple Silicon
✅ **Versioned**: Clear version history and rollback capability
✅ **Tracked**: All builds logged in GitHub Actions
✅ **Fast**: GitHub Actions runners are fast and reliable
✅ **Free**: GitHub Actions is free for public repositories

## Next Steps

1. Complete the setup steps above
2. Create your first release
3. Share your Docker Hub image with users
4. Update your main README.md with installation instructions

---

**Need Help?**
- GitHub Actions Docs: https://docs.github.com/en/actions
- Docker Hub Docs: https://docs.docker.com/docker-hub/
- Open an issue: https://github.com/apollolabsai/MultiPlex-Stats/issues
