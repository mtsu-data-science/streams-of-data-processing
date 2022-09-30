export infra=dev

setup-pre-commit:
	pip install pre-commit
	pre-commit install

spin-up-postgres:
	docker compose -f docker/docker-compose.yml up

run-sensor-ysi:
	poetry run python src/main.py --org epa --filepath data/msds/data/ysi --filetype data --sensor ysi

run-sensor-minidot:
	poetry run python src/main.py --org epa --filepath data/msds/data/minidot --filetype data --sensor minidot

run-sensor-solinst:
	poetry run python src/main.py --org epa --filepath data/msds/data/solinst --filetype data --sensor solinst

run-sensor: run-sensor-ysi run-sensor-minidot run-sensor-solinst

run-log-sheet-ysi:
	poetry run python src/main.py --org epa --filepath data/msds/log-sheet/ysi --filetype log-sheet --sensor ysi

run-log-sheet-minidot:
	poetry run python src/main.py --org epa --filepath data/msds/log-sheet/minidot --filetype log-sheet --sensor minidot

run-log-sheet-solinst:
	poetry run python src/main.py --org epa --filepath data/msds/log-sheet/solinst/ --filetype log-sheet --sensor solinst

run-log-sheet: run-log-sheet-ysi run-log-sheet-minidot run-log-sheet-solinst

run-both: run-sensor-minidot run-log-sheet-minidot

cleanup:
	poetry run black src/
	poetry run isort src/