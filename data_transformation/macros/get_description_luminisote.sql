{#
    Cette macro retourne la description pour les conditions de luminisoté
#}

{% macro get_description_luminosite(lum) -%}

    case lum 
        when 1 then 'Plein jour'
        when 2 then 'Aube'
        when 3 then 'Nuit sans éclairage'
        when 4 then 'Nuit avec éclairage éteint'
        when 5 then 'Nuit avec éclairage allumé'
        else 'Inconnu'
    end 

{%- endmacro %}