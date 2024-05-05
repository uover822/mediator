build :
	mkdir -p app/data
	mkdir -p app/srv
	cp -r ../../../mediator/mediator.js app
	cp -r ../../../mediator/srv/*.js app/srv
	docker build -t msr-mediator:1 .
	docker build -t msr-mediator-prod:1 -f Dockerfile-prod .

clean :
	rm *~

.PHONY : build clean
