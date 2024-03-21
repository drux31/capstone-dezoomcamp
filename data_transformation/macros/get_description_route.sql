{#
    Cette macro retourne la description pour les types de route
#}

{% macro get_description_route(catr) -%}

   case catr
        when 1 then 'Autoroute'
        when 2 then 'Nationale'
        when 3 then 'Départementale'
        when 4 then 'Communales'
        when 5 then 'Hors réseau public'
        when 6 then 'Parkings'
        when 7 then 'Villes'
        when 9 then 'Autre'
        else 'Non renseigné'
    end 

{%- endmacro %}