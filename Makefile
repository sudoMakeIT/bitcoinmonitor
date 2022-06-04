up:
	docker-compose --env-file env up --build -d

down: 
	docker-compose down

clean:
	docker-compose --env-file env down
	docker-compose --env-file env build --no-cache
	docker-compose --env-file env up -d

shell:
	docker exec -ti pipelinerunner bash

logs:
	docker logs -f --tail 1000 pipelinerunner 

pytest:
	docker exec pipelinerunner python3 -m pytest /code/test -vv

stop-etl: 
	docker exec pipelinerunner service cron stop