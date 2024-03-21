{#
    Cette macro retourne la description pour la catégorie de la personne impliquée
#}

{% macro get_description_typeusager(catu) -%}

    case catu 
        when 1 then 'Conducteur'
        when 2 then 'Passager'
        when 3 then 'Piéton'
        else 'Inconnue'
    end

{%- endmacro %}