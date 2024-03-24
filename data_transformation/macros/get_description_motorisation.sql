{#
    Cette macro retourne la description de la motorisation 
#}

{% macro get_description_motorisation(motor) -%}

    case motor  
        when 1 then 'Hydrocarbures'
        when 2 then 'Hybride'
        when 3 then 'Electrique'
        when 4 then 'Hydrogène'
        when 5 then 'Humaine'
        when 6 then 'Autre'
        else 'Inconuue'
    end

{%- endmacro %}