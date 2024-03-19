from data_ingestion.data_extraction import web_to_local
import os
import pytest
from pytest import raises, fail

@pytest.fixture(scope="session")
def create_dataset(tmp_path_factory):
    d = tmp_path_factory.mktemp("sub")
    p = os.path.join(d, 'dataset.txt')
    #p.write(CONTENT)
    with open(p, 'w+') as f:
        f.write("2019,usagers,https://www.data.gouv.fr/fr/datasets/r/36b1b7b3-84b4-4901-9163-59ae8a9e3028\n")
        f.write("2019,vehicules,https://www.data.gouv.fr/fr/datasets/r/780cd335-5048-4bd6-a841-105b44eb2667\n")
        f.write("2019,lieux,https://www.data.gouv.fr/fr/datasets/r/2ad65965-36a1-4452-9c08-61a6c874e3e6\n")
        f.write("2019,caracteristiques,https://www.data.gouv.fr/fr/datasets/r/e22ba475-45a3-46ac-a0f7-9ca9ed1e283a\n")
    return p

@pytest.fixture(scope="session")
def create_db(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data").join("tes_db.db")
    return fn

'''
def test_web_to_local(create_dataset, create_db):
    assert web_to_local(create_dataset, create_db) == 0, \
    'There was a problem with the database creation'
'''

def test_web_to_local_with_no_file_name(create_db):
    with raises(Exception):
        web_to_local('', create_db)
        fail("No value provided for file_name parameter")

def test_web_to_local_with_no_db_name(create_dataset):
    with raises(Exception):
        web_to_local(create_dataset, '')
        fail("No value provided for db_name")

