.PHONY: build

build:
	@python3 -m build

publish: build
	@python3 -m twine upload  dist/*