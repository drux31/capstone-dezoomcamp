select 
    code_commune,
    code_dpt as code_departement,
    nom_commune
from {{ ref("codes_communes") }}