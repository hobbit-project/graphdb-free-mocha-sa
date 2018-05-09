default: build dockerize-sa

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize-sa:
	docker build -t git.project-hobbit.eu:4567/papv/systems/graphdb .
	docker push git.project-hobbit.eu:4567/papv/systems/graphdb

dockerize-graphdb:
	docker build -f graphdb-free.docker -t git.project-hobbit.eu:4567/papv/triplestores/graphdb-free:8.5 .
	docker push git.project-hobbit.eu:4567/papv/triplestores/graphdb-free:8.5
