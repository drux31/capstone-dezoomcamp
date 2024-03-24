{{ config(materialized="view") }}

select 
    Num_Acc as accident_id,
    id_vehicule as vehicule_id, 
    catv as code_categorie_vhl,
    motor as motorisation,
    {{ get_description_motorisation("motor") }} as description_motorisation,
    occutc as nb_occupant
from {{ source("staging", "vehicules_all") }}