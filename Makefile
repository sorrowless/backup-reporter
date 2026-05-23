.PHONY: help prepare build publish push-release

VENV := .venv
PYTHON ?= 3.12
UV ?= uv
GIT_REMOTE ?= origin
PACKAGE_NAME := backup-reporter
POETRY := $(VENV)/bin/poetry
PYPI_TOKEN ?= $(PYPI_API_TOKEN)

export PATH := $(HOME)/.local/bin:$(PATH)

help:
	@echo 'Targets:'
	@echo '  make prepare       - bootstrap uv, venv, and poetry (macOS / Ubuntu)'
	@echo '  make build         - bump version, commit, tag, and build dist/'
	@echo '  make publish       - upload dist/ to PyPI (requires PYPI_API_TOKEN)'
	@echo '  make push-release  - push release commit and tags to origin'

prepare:
	@bash scripts/prepare.sh "$(PYTHON)"

build: prepare
	@. $(VENV)/bin/activate && bash scripts/release-bump.sh

publish: prepare
	@if [ -z "$(PYPI_TOKEN)" ] && [ -z "$$POETRY_PYPI_TOKEN_PYPI" ]; then \
		echo "Set PYPI_API_TOKEN or POETRY_PYPI_TOKEN_PYPI for publish" >&2; \
		exit 1; \
	fi
	@. $(VENV)/bin/activate && \
		export POETRY_KEYRING_BACKEND=keyring.backends.null.Keyring && \
		$(POETRY) config pypi-token.pypi "$${PYPI_API_TOKEN:-$$POETRY_PYPI_TOKEN_PYPI}" && \
		$(POETRY) publish -n

push-release:
	git push $(GIT_REMOTE) HEAD
	git push $(GIT_REMOTE) --tags
