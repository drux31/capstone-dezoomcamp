run: test
	cd data_extraction;./data_extraction.py

test:
	python -m pytest tests