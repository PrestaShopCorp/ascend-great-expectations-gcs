.PHONY: build
export PYTHONPATH := $(PYTHONPATH):/Users/tTrividic/code/python/ascend-great-expectations-gcs/src/ascend_great_expectations_gcs


build:
	@ rm -rf ./dist
	@python3 -m build

publish: build
	@python3 -m twine upload  dist/*

test: 
	