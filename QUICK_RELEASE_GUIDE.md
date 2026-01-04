# Quick Release Guide

## 🚀 Publishing to Docker Hub - TL;DR

### First-Time Setup (Do Once)

1. **Create Docker Hub Repository**
   - Go to https://hub.docker.com
   - Create new repository named `multiplex-stats`
   - Make it public

2. **Generate Access Token**
   - Docker Hub → Account Settings → Security → New Access Token
   - Name: `GitHub Actions`
   - Permissions: Read & Write
   - **SAVE THE TOKEN!**

3. **Add Secrets to GitHub**
   - Go to: https://github.com/apollolabsai/MultiPlex-Stats/settings/secrets/actions
   - Add secret: `DOCKERHUB_USERNAME` = your Docker Hub username
   - Add secret: `DOCKERHUB_TOKEN` = the token from step 2

4. **Update README.md**
   - Replace `YOUR_DOCKERHUB_USERNAME` with your actual username
   - Commit and push

### Creating a Release (Every Time)

#### Option 1: GitHub Web UI (Easiest)

1. Go to: https://github.com/apollolabsai/MultiPlex-Stats/releases/new
2. Create new tag: `v1.0.0` (or next version)
3. Title: `v1.0.0 - Release Title`
4. Description: What's new/changed
5. Click **"Publish release"**
6. Wait 3-5 minutes - check Actions tab for build status
7. Done! Image available at `your-username/multiplex-stats:1.0.0`

#### Option 2: Command Line

```bash
# Create and push tag
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0

# Then create release on GitHub using that tag
```

### Version Numbering

Follow semantic versioning:
- `v1.0.0` → First release
- `v1.0.1` → Bug fix
- `v1.1.0` → New feature
- `v2.0.0` → Breaking change

### What Gets Built

When you create release `v1.2.3`:
- `your-username/multiplex-stats:1.2.3`
- `your-username/multiplex-stats:1.2`
- `your-username/multiplex-stats:1`
- `your-username/multiplex-stats:latest`

Platforms:
- `linux/amd64` (Intel/AMD)
- `linux/arm64` (Apple Silicon, ARM)

### Checking Build Status

- **GitHub Actions**: https://github.com/apollolabsai/MultiPlex-Stats/actions
- **Docker Hub**: https://hub.docker.com/r/your-username/multiplex-stats/tags

### Testing the Published Image

```bash
# Pull latest
docker pull your-username/multiplex-stats:latest

# Run it
docker run -d -p 8487:8487 your-username/multiplex-stats:latest

# Check it works
open http://localhost:8487
```

### Rollback

```bash
# Use specific version
docker pull your-username/multiplex-stats:1.0.0
docker run -d -p 8487:8487 your-username/multiplex-stats:1.0.0
```

## 📋 Pre-Release Checklist

Before creating a release:

- [ ] All changes merged to `main` branch
- [ ] Version number follows semantic versioning
- [ ] CHANGELOG.md updated (if you have one)
- [ ] Tested locally with Docker
- [ ] README.md reflects any new features/changes
- [ ] Docker Hub username updated in README

## 🔄 Automated Triggers

The workflow runs automatically on:

- ✅ **Release creation** → Builds tagged versions + `latest`
- ✅ **Push to main** → Updates `latest` and `main` tags
- ✅ **Push to docker** → Updates `docker` tag (for testing)
- ✅ **Manual trigger** → Run from Actions tab anytime

## 🛠️ Manual Build Trigger

1. Go to: https://github.com/apollolabsai/MultiPlex-Stats/actions/workflows/docker-publish.yml
2. Click **"Run workflow"**
3. Select branch (usually `main` or `docker`)
4. Click **"Run workflow"** button

## 📝 Release Template

Use this template for your release descriptions:

```markdown
## 🚀 MultiPlex Stats v1.0.0

[Brief description of this release]

### ✨ New Features
- Feature 1
- Feature 2

### 🐛 Bug Fixes
- Fix 1
- Fix 2

### 🔧 Changes
- Change 1
- Change 2

### 📦 Docker Installation

```bash
docker pull your-username/multiplex-stats:1.0.0
docker run -d -p 8487:8487 -v multiplex-data:/app/instance your-username/multiplex-stats:1.0.0
```

Or update your docker-compose.yml:
```yaml
services:
  multiplex-stats:
    image: your-username/multiplex-stats:1.0.0
```

### 🔗 Links
- Docker Hub: https://hub.docker.com/r/your-username/multiplex-stats
- Documentation: https://github.com/apollolabsai/MultiPlex-Stats
```

## 🚨 Troubleshooting

### "Error: Process completed with exit code 1"
- Check Actions logs for specific error
- Usually a build failure - check Dockerfile syntax
- Verify all dependencies in requirements.txt are available

### "Error: Failed to push image"
- Verify GitHub secrets are set correctly
- Regenerate Docker Hub token if needed
- Check token has Read & Write permissions

### "Image not appearing on Docker Hub"
- Wait 1-2 minutes after workflow completes
- Check workflow completed successfully (green checkmark)
- Verify Docker Hub repository exists and is accessible

## 📞 Need Help?

See the full guide: [DOCKER_HUB_SETUP.md](./DOCKER_HUB_SETUP.md)
