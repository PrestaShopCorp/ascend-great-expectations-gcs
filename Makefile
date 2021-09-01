.PHONY: build

build:
	@ rm -rf ./dist
	@python3 -m build

publish: build
	@python3 -m twine upload  dist/*