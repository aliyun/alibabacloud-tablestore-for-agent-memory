GIT_ROOT ?= $(shell git rev-parse --show-toplevel)

help:	## Show all Makefile targets.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[33m%-30s\033[0m %s\n", $$1, $$2}'

format:	## Run code auto formatters (spotless).
	./mvnw clean spotless:apply

lint:
	./mvnw clean spotless:check

build:
	./mvnw clean source:jar javadoc:jar package -DskipTests

deploy:
	./mvnw clean package deploy -DskipTests  -Dgpg.skip -Pinclude-spring-ai

test:	## Run tests via pytest.
	./mvnw test
