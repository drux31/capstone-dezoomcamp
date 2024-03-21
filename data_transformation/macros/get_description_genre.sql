{#
    Cette macro retourne la description pour le sexe de la personne impliquée
#}

{% macro get_description_genre(sexe) -%}

    case sexe
        when 1 then 'Masculin'
        when 2 then 'Feminin'
        else 'Inconnu'
    end 

{%- endmacro %}