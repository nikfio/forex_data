# GitHub Pages Integration for Sphinx Documentation

This guide explains how to integrate your Sphinx documentation with GitHub Pages.

## Overview

Your Sphinx documentation is located in the `docs/` directory and will be automatically built and deployed to GitHub Pages using GitHub Actions.

## Setup Steps

### 1. Enable GitHub Pages in Repository Settings

1. Go to your repository on GitHub: `https://github.com/nikfio/forex_data`
2. Click on **Settings** → **Pages** (in the left sidebar)
3. Under **Source**, select:
   - **Source**: `GitHub Actions` (not "Deploy from a branch")
4. Click **Save**

### 2. Verify GitHub Actions Workflow

The workflow file `.github/workflows/docs.yml` has been created. It will:

- Trigger on every push to the `master` branch
- Install Python 3.12 and Poetry
- Install project dependencies
- Build Sphinx documentation (`sphinx-build -b html`)
- Deploy to GitHub Pages automatically

### 3. Push Changes to GitHub

```bash
git add .github/workflows/docs.yml
git commit -m "Add GitHub Actions workflow for Sphinx documentation"
git push origin master
```

### 4. Monitor the Build

1. Go to the **Actions** tab in your GitHub repository
2. You should see a workflow run called "Build and Deploy Sphinx Documentation"
3. Click on it to see the build progress
4. Once completed, your documentation will be available at:
   ```
   https://nikfio.github.io/forex_data/
   ```

## Manual Build and Testing Locally

Before pushing, you can test the documentation build locally:

```bash
# Navigate to docs directory
cd docs

# Build the documentation
poetry run sphinx-build -b html source build/html

# Open the built documentation in your browser
# On macOS:
open build/html/index.html

# On Linux:
xdg-open build/html/index.html
```

## Updating Documentation

Any time you:
- Update docstrings in your code
- Modify `.rst` files in `docs/source/`
- Change Sphinx configuration in `docs/source/conf.py`

Simply commit and push to master:

```bash
git add .
git commit -m "Update documentation"
git push origin master
```

The GitHub Actions workflow will automatically rebuild and redeploy your documentation.

## Troubleshooting

### Build Fails in GitHub Actions

1. Check the Actions tab for error messages
2. Common issues:
   - Missing dependencies in `pyproject.toml`
   - Syntax errors in `.rst` files
   - Import errors in Python code

### Documentation Not Updating

1. Check that the workflow completed successfully in the Actions tab
2. GitHub Pages can take a few minutes to update after deployment
3. Try hard-refreshing your browser (Ctrl+Shift+R or Cmd+Shift+R)
4. Check that GitHub Pages is enabled and set to "GitHub Actions" source

### Permissions Error

If you see a permissions error in GitHub Actions:
1. Go to **Settings** → **Actions** → **General**
2. Under **Workflow permissions**, select "Read and write permissions"
3. Check "Allow GitHub Actions to create and approve pull requests"
4. Click **Save**

## Alternative: Manual Deployment (Not Recommended)

If you prefer manual deployment instead of GitHub Actions:

### Option 1: Using gh-pages branch

```bash
# Install ghp-import
pip install ghp-import

# Build docs
cd docs
poetry run sphinx-build -b html source build/html

# Deploy to gh-pages branch
poetry run ghp-import -n -p -f build/html
```

Then in GitHub Settings → Pages, select:
- Source: Deploy from a branch
- Branch: `gh-pages` / `root`

### Option 2: Using docs/ folder on master

1. Build docs to a different location:
   ```bash
   poetry run sphinx-build -b html docs/source docs
   ```

2. In GitHub Settings → Pages, select:
   - Source: Deploy from a branch
   - Branch: `master` / `/docs`

**Note**: The GitHub Actions approach (recommended) is more reliable and automated.

## Additional Resources

- [GitHub Pages Documentation](https://docs.github.com/en/pages)
- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

## Current Configuration

Your Sphinx configuration (`docs/source/conf.py`) includes:
- Read the Docs theme (`sphinx_rtd_theme`)
- Autodoc for automatic API documentation
- Napoleon for Google-style docstrings
- GitHub Pages extension (`sphinx.ext.githubpages`)
- Type hints support (`sphinx_autodoc_typehints`)

## Next Steps

1. Enable GitHub Pages in repository settings (Source: GitHub Actions)
2. Push the new workflow file to GitHub
3. Wait for the workflow to complete
4. Visit `https://nikfio.github.io/forex_data/` to see your documentation live!
