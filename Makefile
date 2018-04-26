default: build dockerize

build:	
	mvn clean package -U -Dmaven.test.skip=true

dockerize:
	docker build -t git.project-hobbit.eu:4567/papv/systems/graphdb .

	docker push git.project-hobbit.eu:4567/papv/systems/graphdb

