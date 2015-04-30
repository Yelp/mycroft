.PHONY: test

test: .mycroft-test-docker-built mycroft/tests/dynamodb_local/DynamoDBLocal.jar
	docker run -i -t -v $(realpath mycroft):/mycroft -v $(realpath mycroft_config):/mycroft_config mycroft-dev py.test-2.7
	
clean: .mycroft-test-docker-built
	docker run -i -t -v $(realpath mycroft):/mycroft -v $(realpath mycroft_config):/mycroft_config mycroft-dev make clean

.mycroft-test-docker-built: .mycroft-docker-built Dockerfile.dev mycroft/requirements-dev.txt
	docker build -t mycroft-dev -f Dockerfile.dev .
	touch .mycroft-test-docker-built

mycroft/tests/dynamodb_local/DynamoDBLocal.jar:
	mkdir -p mycroft/tests/dynamodb_local/
	curl -L http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz | tar xz -C mycroft/tests/dynamodb_local/

.mycroft-docker-built: Dockerfile mycroft/requirements.txt mycroft/requirements-emr.txt mycroft/requirements-custom.txt
	docker build -t mycroft .
	touch .mycroft-docker-built
