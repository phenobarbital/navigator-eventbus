# navigator-eventbus Makefile

.PHONY: venv install develop setup dev release format lint test clean distclean \
		lock update info add add-dev remove build help \
		bump-patch bump-minor bump-major

# Python version to use
PYTHON_VERSION := 3.11

# Enforce virtual environment usage
export PIP_REQUIRE_VIRTUALENV=true

# Auto-detect available tools
HAS_UV := $(shell command -v uv 2> /dev/null)

# Detect OS
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    OS_TYPE := Linux
endif
ifeq ($(UNAME_S),Darwin)
    OS_TYPE := MacOS
endif

# Install uv for faster workflows
install-uv:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	@echo "uv installed! You may need to restart your shell or run 'source ~/.bashrc'"

# Create virtual environment
venv:
	uv venv --python $(PYTHON_VERSION) .venv
	@echo 'run `source .venv/bin/activate` to start developing'

# Install production dependencies (no dev, no extras)
install:
	uv sync --frozen --no-dev

# Install with all optional extras
install-all:
	uv sync --frozen --no-dev --all-extras

# Install in development mode with dev dependencies
develop:
	uv sync --all-extras
	@echo "Development environment ready."

# Setup development environment from requirements file
setup:
	uv pip install -r requirements/requirements-dev.txt

# Install in development mode using flit
dev:
	uv pip install flit
	flit install --symlink

# Generate lock file
lock:
ifdef HAS_UV
	uv lock
else
	@echo "Lock files require uv. Install with: make install-uv"
endif

# Update all dependencies
update:
	uv lock --upgrade

# Show dependency tree
info:
	uv tree

# Add new dependency and update lock file
add:
	@if [ -z "$(pkg)" ]; then echo "Usage: make add pkg=package-name"; exit 1; fi
	uv add $(pkg)

# Add development dependency
add-dev:
	@if [ -z "$(pkg)" ]; then echo "Usage: make add-dev pkg=package-name"; exit 1; fi
	uv add --dev $(pkg)

# Remove dependency
remove:
	@if [ -z "$(pkg)" ]; then echo "Usage: make remove pkg=package-name"; exit 1; fi
	uv remove $(pkg)

# Format code
format:
	uv run ruff format src/navigator_eventbus tests

# Lint code
lint:
	uv run ruff check src/navigator_eventbus tests
	uv run mypy src/navigator_eventbus

# Run tests with coverage
test:
	uv run pytest

# Build package
build: clean
	uv build

# Build and publish to PyPI
release: lint test clean
	uv build
	uv publish dist/navigator_eventbus-*.tar.gz dist/navigator_eventbus-*.whl

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete
	find . -type d -name __pycache__ -delete
	@echo "Clean complete."

# Remove virtual environment
distclean: clean
	rm -rf .venv

# ---------------------------------------------------------------------------
# Version management
# ---------------------------------------------------------------------------
VERSION_FILE := src/navigator_eventbus/__init__.py

define _bump
	@python -c "import re; \
	content = open('$(1)').read(); \
	version = re.search(r'__version__ = \"(.+)\"', content).group(1); \
	parts = version.split('.'); \
	idx = $(2); \
	parts[idx] = str(int(parts[idx]) + 1); \
	parts[idx+1:] = ['0'] * len(parts[idx+1:]); \
	new_version = '.'.join(parts); \
	new_content = re.sub(r'__version__ = \".+\"', f'__version__ = \"{new_version}\"', content); \
	open('$(1)', 'w').write(new_content); \
	print(f'$(1): {version} → {new_version}')"
endef

bump-patch:
	$(call _bump,$(VERSION_FILE),2)

bump-minor:
	$(call _bump,$(VERSION_FILE),1)

bump-major:
	$(call _bump,$(VERSION_FILE),0)

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help:
	@echo "Available targets:"
	@echo ""
	@echo "  Install:"
	@echo "    install         - Install production dependencies"
	@echo "    install-all     - Install with all optional extras"
	@echo "    develop         - Install in dev mode with all extras"
	@echo ""
	@echo "  Development:"
	@echo "    venv            - Create virtual environment"
	@echo "    format          - Format code (ruff)"
	@echo "    lint            - Lint code (ruff + mypy)"
	@echo "    test            - Run tests (pytest)"
	@echo ""
	@echo "  Build & Release:"
	@echo "    build           - Build package"
	@echo "    release         - Build and publish to PyPI"
	@echo "    clean           - Clean build artifacts"
	@echo "    distclean       - Clean everything including .venv"
	@echo ""
	@echo "  Version:"
	@echo "    bump-patch      - Bump patch version (0.1.x)"
	@echo "    bump-minor      - Bump minor version (0.x.0)"
	@echo "    bump-major      - Bump major version (x.0.0)"
	@echo ""
	@echo "  Dependencies:"
	@echo "    lock            - Generate lock file"
	@echo "    update          - Update all dependencies"
	@echo "    info            - Show dependency tree"
	@echo "    add pkg=X       - Add dependency"
	@echo "    add-dev pkg=X   - Add dev dependency"
	@echo "    remove pkg=X    - Remove dependency"
	@echo ""
	@echo "  System:"
	@echo "    install-uv      - Install uv package manager"
	@echo ""
	@echo "Current setup: Python $(PYTHON_VERSION)"
