from data_ingestion.data_extraction import web_to_local
import pathlib
from pytest import raises, fail

file_name = 'dataset.txt'
years = ['2019']
db_name = 'test_db.db'

def test_web_to_local():
    assert web_to_local(file_name, years, db_name) == 0, \
    'There was a problem with the database creation'

def test_web_to_local_with_no_file_name():
    with raises(Exception):
        web_to_local('', years, db_name)
        fail("No value provided for file_name parameter")

def test_web_to_local_with_no_years():
    with raises(Exception):
        web_to_local(file_name, [], db_name)
        fail("Empty table provided for years parameter")

def test_web_to_local_with_no_db_name():
    with raises(Exception):
        web_to_local(file_name, years, '')
        fail("No value provided for db_name")

