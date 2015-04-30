.PHONY: all production test docs clean aws/src-tree.tar.gz

SRC=sherlock
BATCH=$(SRC)/batch
SCHEMA=schema
COMMON=$(SRC)/common

all: production

production: emr
	@true

docs:
	tox -e docs

itest:
	# TODO make integration test
	tox

test: emr
	tox


clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf aws/python-modules/
	rm -f aws/src-tree.tar.gz
	rm -rf tests/mr_output/
	rm -rf tests/oddjob_output/
	rm -f $(SCHEMA)/temp_schema.yaml $(SCHEMA)/temp_schema.sql

emr_trigger = $(emr_files) $(shell for i in **/*.py; do echo $$i; done | sed '/ /d')

aws/python-modules/required-packages-present: requirements-emr.txt
	rm -rf aws/python-modules/*
	mkdir -p aws/python-modules/
	pip install --no-use-wheel -d aws/python-modules -r requirements-emr.txt
	touch aws/python-modules/required-packages-present

# For EMR, we need to package up everything in requirements-emr.txt and all of our own source code
aws/src-tree.tar.gz: $(emr_trigger) aws/python-modules/required-packages-present
	tar caf aws/src-tree.tar.gz $(SRC)/__init__.py $(BATCH)/ $(COMMON)/ --exclude *.pyc --exclude __pycache__

emr: aws/src-tree.tar.gz
