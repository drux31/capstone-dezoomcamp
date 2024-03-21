
select 
    code_categ as code_categorie_vhl,
    libelle_categ as libelle_categorie_vhl
from {{ ref("categories_vhl") }}