{{ config(materialized="view") }}

select 
    Num_Acc as accident_id,
    Accident_date as accident_date,
    lum as luminisote,
    {{ get_description_luminosite("lum") }} as description_luminosite,
    dep as code_departement,
    com as code_commune,
    agg as localisation,
    case agg
        when 1 then 'Hors agglomération'
        when 2 then 'En agglomération'
        else 'Inconnu'
    end as description_localisation,
    --int,
    atm as cond_atmospherique,
    {{ get_description_atm("atm") }} as dsc_cond_admonspherique,
    adr as adresse_postale,
    latitude,
    longitude
from  {{ source("staging", "caracteristiques_all") }}