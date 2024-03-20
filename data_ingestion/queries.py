# lieux
q_lieux_2021 = """
    select 
        Num_Acc,
        catr,
        voie,
        v1,
        v2,
        circ,
        cast(nullif(nbv, '#ERREUR') as BIGINT) as nbv,
        vosp,
        prof,
        pr,
        pr1,
        plan,
        cast(replace(lartpc, ',', '.') as double) as lartpc,
        cast(replace(larrout, ',', '.') as double) as larrout,
        surf,
        infra,
        situ,
        vma
    from raw_data.lieux_2022
    
    """
q_lieux_2022 = """
    select 
        Num_Acc,
        catr,
        voie,
        v1,
        v2,
        circ,
        cast(nullif(nbv, '#ERREUR') as BIGINT) as nbv,
        vosp,
        prof,
        pr,
        pr1,
        plan,
        cast(replace(lartpc, ',', '.') as double) as lartpc,
        cast(replace(larrout, ',', '.') as double) as larrout,
        surf,
        infra,
        situ,
        vma
    from raw_data.lieux_2022
    
    """
q_lieux_2020 = """
    select 
        Num_Acc,
        catr,
        voie,
        v1,
        v2,
        circ,
        cast(nullif(nbv, '#ERREUR') as BIGINT) as nbv,
        vosp,
        prof,
        pr,
        pr1,
        plan,
        cast(replace(lartpc, ',', '.') as double) as lartpc,
        cast(replace(larrout, ',', '.') as double) as larrout,
        surf,
        infra,
        situ,
        vma
    from raw_data.lieux_2022
    
    """
q_lieux_2019 = """
    select 
        Num_Acc,
        catr,
        voie,
        v1,
        v2,
        circ,
        nbv,
        vosp,
        prof,
        pr,
        pr1,
        plan,
        lartpc,
        larrout,
        surf,
        infra,
        situ,
        vma
    from raw_data.lieux_2019
    """

# Lieux from 2019 to 2021
query_union_lieux = " ".join([q_lieux_2021, 
                       "union all by name",
                       q_lieux_2020,
                       "union all by name",
                       q_lieux_2019])

#usagers from 2019 to 2021
query_union_usagers = " ".join([
            "select * from raw_data.usagers_2019",
            "union all by name",
            "select * from raw_data.usagers_2020",
            "union all by name",
            "select * from raw_data.usagers_2021"])

#caractéristiques from 2019 to 2022
query_carac_2019 = """
    select 
        Num_Acc,
        cast(concat(an, '-', mois, '-', jour) as date) as Accident_date,
        jour,
        mois,
        an as annee,
        hrmn,
        lum,
        dep,
        com,
        agg,
        int as intersection,
        atm,
        col,
        adr,
        cast(replace(lat, ',', '.') as float) as latitude,
        cast(replace(long, ',', '.') as float) as longitude
    from raw_data.caracteristiques_2019
    """
query_carac_2020 = """
    select 
        Num_Acc,
        cast(concat(an, '-', mois, '-', jour) as date) as Accident_date,
        jour,
        mois,
        an as annee,
        hrmn,
        lum,
        dep,
        com,
        agg,
        int as intersection,
        atm,
        col,
        adr,
        cast(replace(lat, ',', '.') as float) as latitude,
        cast(replace(long, ',', '.') as float) as longitude
    from raw_data.caracteristiques_2020
    """
query_carac_2021 = """
    select 
        Num_Acc,
        cast(concat(an, '-', cast(mois as BIGINT), '-', jour) as date) as Accident_date,
        jour,
        cast(mois as BIGINT) as mois,
        an as annee,
        hrmn,
        lum,
        dep,
        com,
        agg,
        int as intersection,
        atm,
        col,
        adr,
        cast(replace(lat, ',', '.') as float) as latitude,
        cast(replace(long, ',', '.') as float) as longitude
    from raw_data.caracteristiques_2021
    """
query_carac_2022 = """
    select 
        Accident_Id as Num_Acc,
        cast(concat(an, '-', cast(mois as BIGINT), '-', jour) as date) as Accident_date,
        jour,
        cast(mois as BIGINT) as mois,
        an as annee,
        hrmn,
        lum,
        dep,
        com,
        agg,
        int as intersection,
        atm,
        col,
        adr,
        cast(replace(lat, ',', '.') as float) as latitude,
        cast(replace(long, ',', '.') as float) as longitude
    from raw_data.caracteristiques_2022
    """

query_union_carac = " ".join([
    query_carac_2019,
    "union all by name",
    query_carac_2020,
    "union all by name",
    query_carac_2021
])

# vehicules from 2019 to 2022
query_vhl_2019 = """
    select 
        Num_Acc,
        id_vehicule, 
        num_veh,
        senc,
        catv,
        obs,
        obsm,
        choc,
        manv,
        motor,
        occutc
    from raw_data.vehicules_2019
    """
query_vhl_2020 = """
    select 
        Num_Acc,
        id_vehicule, 
        num_veh,
        senc,
        catv,
        obs,
        obsm,
        choc,
        manv,
        motor,
        occutc
    from raw_data.vehicules_2020
    """
query_vhl_2021 = """
    select 
        Num_Acc,
        id_vehicule, 
        num_veh,
        senc,
        catv,
        obs,
        obsm,
        choc,
        manv,
        motor,
        occutc
    from raw_data.vehicules_2021
    """
query_vhl_2022 = """
    select 
        Num_Acc,
        id_vehicule, 
        num_veh,
        senc,
        catv,
        obs,
        obsm,
        choc,
        manv,
        motor,
        occutc
    from raw_data.vehicules_2022
    """
query_union_vhl = " ".join([
    query_vhl_2019,
    "union all by name",
    query_vhl_2020,
    "union all by name",
    query_vhl_2021
])

query_seq = """
create or replace sequence staging.dw_log_table_id_sequence start 1
"""

query_dw_log_table = """
create or replace table staging.dw_log_table (
    id int default nextval('staging.dw_log_table_id_sequence'),
    schema_name varchar not null,
    table_name varchar unique not null,
    table_row_count int not null,
    last_update date not null
)
"""

query_insert_dw_log_table = """
insert into staging.dw_log_table (schema_name, table_name, table_row_count, last_update)
select 'staging', 'usagers_all', count(*), '2021-12-31' from staging.usagers_all"
"""