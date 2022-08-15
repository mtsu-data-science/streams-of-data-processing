setup-pre-commit:
	pip install pre-commit
	pre-commit install

spin-up-postgres:
	docker compose -f docker/docker-compose.yml up