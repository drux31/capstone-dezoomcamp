{#
    Cette macro retourne la description pour la gravité de l''accident pour l''usager
#}

{% macro get_description_gravite(grav) -%}

    case grav 
        when 1 then 'Indemne'
        when 2 then 'Tué'
        when 3 then 'Blessé hospitalisé'
        when 4 then 'Blessé léger'
        else 'Inconnue'
    end

{%- endmacro %}