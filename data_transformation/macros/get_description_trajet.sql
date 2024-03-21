{#
    Cette macro retourne la description pour le trajet de l''accident
#}

{% macro get_description_trajet(trajet) -%}

    case trajet 
        when 1 then 'Domicile - travail' 
        when 2 then 'Domicile - école'
        when 3 then 'Courses'
        when 4 then 'Déplacement professionnel'
        when 5 then 'Loisirs'
        when 9 then 'Autre'
        else 'Non renseigné' 
    end

{%- endmacro %}