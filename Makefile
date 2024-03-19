run: test
	cd data_ingestion;./data_extraction.py

test:
	python -m pytest tests/code