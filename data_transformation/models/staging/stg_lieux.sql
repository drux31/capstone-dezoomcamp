{{ config(materialized="view") }}

select 
    Num_Acc as accident_id,
    catr as categorie_route,
    {{ get_description_route("catr") }} as dsc_categorie_route,
    voie as numero_route,
    vma as vitesse_max
from  {{ source("staging", "lieux_all") }}