up:
	docker-compose --env-file env up --build

down:
	docker-compose down

clean:
	docker-compose --env-file env down
	docker-compose --env-file env build --no-cache
	docker-compose --env-file env up

shell:
	docker exec -ti airflow-airflow-worker-1 bash

logs:
	docker logs -f --tail 1000 airflow-airflow-worker-1
