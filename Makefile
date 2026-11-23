.PHONY: up down test demo demo-full logs status
up:
	docker compose up -d --build
down:
	docker compose down
test:
	docker compose run --rm --no-deps wms python -m pytest -q
demo:
	python3 scripts/demo.py
demo-full:
	python3 scripts/demo.py --full
logs:
	docker compose logs -f consumer
status:
	docker compose ps
