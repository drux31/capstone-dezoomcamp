select 
    code_dept as code_departement,
    nom_dept as nom_departement,
    nom_region
from {{ ref("codes_departements") }}