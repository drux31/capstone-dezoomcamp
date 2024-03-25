with donnees_vehicules as (

    select *
    from {{ ref("stg_vehicules") }}
    where description_motorisation != 'Inconnue'
),
donnees_lieux as (

    select *
    from {{ ref("stg_lieux") }}
    where dsc_categorie_route != 'Non renseigné'   
),
donnees_caracteristiques as (

    select * 
    from {{ ref("stg_caracteristiques") }}
    where accident_date is not null
),
donnees_usagers as (

    select *
    from {{ ref("stg_usagers") }}
),
dim_categ_vhl as (

    select *
    from {{ ref("dim_categories_vhl") }}
),
dim_communes as (

    select *
    from {{ ref("dim_communes") }}
),
dim_dpt as (

    select *
    from {{ ref("dim_departements") }}
),
usagers_accident as (

    select 
        accident_id,
        count(*) as nb_usagers_par_accidents
    from donnees_usagers
    group by 1
),
vehicules_accident as (

    select 
        accident_id,
        count(*) as nb_vehicules_par_accidents
    from donnees_vehicules
    group by 1
),
final as (
    select 
        ca.accident_id,
        ca.accident_date,
        ca.cond_atmospherique,
        ca.dsc_cond_admonspherique,
        ca.luminisote,
        ca.description_luminosite,
        dl.categorie_route,
        dl.dsc_categorie_route,
        dp.code_departement,
        dp.nom_departement,
        dp.nom_region,
        dc.nom_commune,
        ca.latitude,
        ca.longitude,
        dv.motorisation,
        dv.description_motorisation,
        dv.nb_occupant as nb_occupant_vhl_public,
        dv.code_categorie_vhl,
        dcv.libelle_categorie_vhl,
        du.annee_naissance as annee_naissance_usager,
        du.genre_usager,
        du.description_genre_usager,
        du.gravite_blessure,
        du.description_gravite,
        du.categorie_usager,
        du.description_categorie_usager,
        du.trajet_accident,
        du.description_trajet,
        coalesce(ua.nb_usagers_par_accidents, 0) as nb_usagers_par_accidents,
        coalesce(va.nb_vehicules_par_accidents, 0) as nb_vehicules_par_accidents
    from donnees_caracteristiques ca
    
    join dim_dpt dp on dp.code_departement = ca.code_departement
    join dim_communes dc on (dc.code_departement = dp.code_departement and dc.code_commune = ca.code_commune)
    join donnees_lieux dl on dl.accident_id = ca.accident_id
    join donnees_usagers du on du.accident_id = ca.accident_id
    join donnees_vehicules dv on (dv.accident_id = ca.accident_id and du.vehicule_id = dv.vehicule_id)
    join dim_categ_vhl dcv on dcv.code_categorie_vhl = dv.code_categorie_vhl
    left join usagers_accident ua on ua.accident_id = ca.accident_id
    left join vehicules_accident va on va.accident_id = ca.accident_id
)
select * from final
