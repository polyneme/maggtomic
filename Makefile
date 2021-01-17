init:
	pip install --editable .
	pip install --upgrade -r requirements/main.txt  -r requirements/dev.txt
	rm -rf .tox

update-deps:
	pip install --upgrade pip-tools pip setuptools
	pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/main.txt requirements/main.in
	pip-compile --upgrade --build-isolation --generate-hashes --output-file requirements/dev.txt requirements/dev.in

update: update-deps init

publish:
	invoke publish

.PHONY: update-deps init update publish