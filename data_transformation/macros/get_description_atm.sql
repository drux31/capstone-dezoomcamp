{#
    Cette macro retourne la description pour les conditions atmosphériques
#}

{% macro get_description_atm(atm) -%}

    case atm 
        when 1 then 'Normale'
        when 2 then 'Pluie légère'
        when 3 then 'Pluie forte'
        when 4 then 'Neige - grêle'
        when 5 then 'Brouillard'
        when 6 then 'Tempête'
        when 8 then 'Temps couvert'
        when 7 then 'Temps éblouissant'
        else 'Non renseigné'
    end

{%- endmacro %}