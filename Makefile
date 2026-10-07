.PHONY: up down logs test clean

up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f article-processor

test:
	docker compose --profile test run --rm test

clean:
	docker compose down -v --remove-orphans
