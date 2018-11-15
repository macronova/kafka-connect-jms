include environment

NS = macronova
IMAGE_NAME = kafka-connect-jms
VERSION = latest

CONTAINER_NAME = connect-jms
CONTAINER_INSTANCE = default

.PHONY: build push shell run start stop rm release clean

build:
	docker build -t $(NS)/$(IMAGE_NAME):$(VERSION) -f Dockerfile .

push:
	docker push $(NS)/$(IMAGE_NAME):$(VERSION)

shell:
	docker run --rm --name $(CONTAINER_NAME)-$(CONTAINER_INSTANCE) -i -t $(PORTS) $(VOLUMES) $(ENV) $(NS)/$(IMAGE_NAME):$(VERSION) /bin/bash

run:
	docker run --rm --name $(CONTAINER_NAME)-$(CONTAINER_INSTANCE) $(PORTS) $(VOLUMES) $(ENV) $(NS)/$(IMAGE_NAME):$(VERSION)

start:
	docker start $(CONTAINER_NAME)-$(CONTAINER_INSTANCE)

stop:
	docker stop $(CONTAINER_NAME)-$(CONTAINER_INSTANCE)

rm:
	docker rm $(CONTAINER_NAME)-$(CONTAINER_INSTANCE)

release: build
	make push -e VERSION=$(VERSION)

clean:
	-docker stop $(CONTAINER_NAME)-$(CONTAINER_INSTANCE)
	-docker rm $(CONTAINER_NAME)-$(CONTAINER_INSTANCE)
	-docker rmi $(NS)/$(IMAGE_NAME):$(VERSION)

default: build
