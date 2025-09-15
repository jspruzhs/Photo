# -------- Configuration --------
PYTHON      ?= python3.11
VENV_DIR    ?= .venv
PY          := $(VENV_DIR)/bin/$(PYTHON)
PIP         := $(VENV_DIR)/bin/pip

# Default goal shows help
.DEFAULT_GOAL := help

# -------- Helpers --------
$(VENV_DIR)/bin/activate:  ## Create virtual env (./.venv)
	$(PYTHON) -m venv $(VENV_DIR)
	$(PIP) install --upgrade pip wheel

.PHONY: venv
venv: $(VENV_DIR)/bin/activate  ## Ensure venv exists

.PHONY: install
install: venv  ## Install Python dependencies from requirements.txt
	$(PIP) install -r requirements.txt

# -------- Main workflows --------
.PHONY: scan
scan: install  ## Scan inbox/, make previews, sync data/photos.xlsx (auto-enable *_enabled)
	$(PY) scripts/update_photos.py

.PHONY: describe
describe: install  ## Generate/refresh base_title + base_description + base_tags (uses previews)
	$(PY) scripts/update_photos.py --describe $(if $(LIMIT),--describe-limit $(LIMIT),)

.PHONY: reset
reset: install  ## Clear all rows from Excel (keep headers)
	$(PY) scripts/update_photos.py --reset

.PHONY: adapt-ss
adapt-ss: install  ## Adapt base_* -> SS_* (title/description/tags) for Shutterstock
	$(PY) scripts/adapt_shutterstock.py \
		$(if $(LIMIT),--limit $(LIMIT),) \
		$(if $(FORCE),--force,) \
		$(if $(DRY_RUN),--dry-run,)

# Convenience wrappers
.PHONY: adapt-ss-force
adapt-ss-force:  ## Force-regenerate SS_* even if already filled
	$(MAKE) adapt-ss FORCE=1 $(if $(LIMIT),LIMIT=$(LIMIT),)

.PHONY: adapt-ss-dry
adapt-ss-dry:  ## Dry run: compute SS_* without writing to disk
	$(MAKE) adapt-ss DRY_RUN=1 $(if $(LIMIT),LIMIT=$(LIMIT),)

.PHONY: all
all: scan describe adapt-ss  ## Full pipeline: scan -> describe -> SS adapt

# -------- Housekeeping --------
.PHONY: clean
clean:  ## Remove caches (keeps .venv and your data/)
	@find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	@find . -name "*.pyc" -delete

.PHONY: help
help:  ## Show this help
	@printf "\nUsage: make <target> [LIMIT=N] [FORCE=1] [DRY_RUN=1]\n\n"
	@printf "Targets:\n"
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@printf "\nExamples:\n"
	@printf "  make scan\n"
	@printf "  make describe LIMIT=25\n"
	@printf "  make adapt-ss\n"
	@printf "  make adapt-ss FORCE=1 LIMIT=50\n"
	@printf "  make reset\n\n"
