GIT_ROOT ?= $(shell git rev-parse --show-toplevel)

help:	## Show all Makefile targets.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[33m%-30s\033[0m %s\n", $$1, $$2}'

format:	## Run code auto formatters (black).
	poetry run isort **/*.py
	poetry run autoflake --remove-unused-variables --expand-star-imports --remove-all-unused-imports --ignore-pass-after-docstring --in-place **/*py
	poetry run black **/*.py 
	poetry run black **/*.ipynb 

lint:
	poetry run autoflake --remove-unused-variables --expand-star-imports --remove-all-unused-imports --ignore-pass-after-docstring -cd **/*py
	poetry run black **/*.py --check --diff
	poetry run black **/*.ipynb --check --diff
test:	## Run tests via pytest.
	pytest tests
