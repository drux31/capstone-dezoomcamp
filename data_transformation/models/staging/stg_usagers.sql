{{ config(materialized="view") }}

select 
    Num_Acc as accident_id,
    id_vehicule as vehicule_id,
    catu as categorie_usager,      
    {{ get_description_typeusager("catu") }} as description_categorie_usager,
    grav as gravite_blessure,        
    {{ get_description_gravite("grav") }} as description_gravite,
    sexe as genre_usager, 
    {{ get_description_genre("sexe") }} as description_genre_usager,       
    an_nais as annee_naissance,
    trajet as trajet_accident,
    {{ get_description_trajet("trajet") }} as description_trajet,        
from  {{ source("staging", "usagers_all") }}
where id_vehicule is not null