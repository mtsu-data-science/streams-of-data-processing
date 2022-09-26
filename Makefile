export infra=dev

setup-pre-commit:
	pip install pre-commit
	pre-commit install

spin-up-postgres:
	docker compose -f docker/docker-compose.yml up

run-sensor:
	poetry run python src/main.py --org epa --filepath data/msds/sensor --filetype sensor

run-log-sheet:
	poetry run python src/main.py --org epa --filepath data/msds/log-sheet --filetype log-sheet

run-both: run-sensor run-log-sheet

cleanup:
	poetry run black src/
	poetry run isort src/