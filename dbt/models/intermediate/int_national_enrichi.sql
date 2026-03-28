-- Enrichissement des données nationales avec dimensions temporelles et KPIs
-- stg_eco2mix_national

with source as (

    select *
    from {{ ref('stg_eco2mix_national') }}

),

enriched as (

    select
        -- Colonnes de base
        date_heure,
        consommation_mw,
        prevision_j1_mw,
        prevision_j_mw,
        nucleaire_mw,
        hydraulique_mw,
        eolien_mw,
        solaire_mw,
        gaz_mw,
        fioul_mw,
        charbon_mw,
        bioenergies_mw,
        pompage_mw,
        ech_physiques_mw,
        taux_co2,
        _loaded_at,

        -- Dimensions temporelles
        hour(date_heure)                            as heure,

        -- Jour de la semaine en français
        case dayofweek(date_heure)
            when 0 then 'Dimanche'
            when 1 then 'Lundi'
            when 2 then 'Mardi'
            when 3 then 'Mercredi'
            when 4 then 'Jeudi'
            when 5 then 'Vendredi'
            when 6 then 'Samedi'
        end                                         as jour_semaine,

        -- Flag weekend (1 = samedi ou dimanche)
        case when dayofweek(date_heure) in (0, 6)
            then 1 else 0
        end                                         as flag_weekend,

        -- Mois (1-12)
        month(date_heure)                           as mois,

        -- Année
        year(date_heure)                            as annee,

        -- Saison 
        case
            when month(date_heure) in (12, 1, 2)  then 'Hiver'
            when month(date_heure) in (3, 4, 5)   then 'Printemps'
            when month(date_heure) in (6, 7, 8)   then 'Été'
            when month(date_heure) in (9, 10, 11) then 'Automne'
        end                                         as saison,

        -- Flags événements historiques
        -- Covid : premier confinement français
        case when date_heure between '2020-03-17' and '2020-05-11'
            then 1 else 0
        end                                         as flag_covid,

        -- Crise nucléaire 2022 : arrêts massifs de réacteurs
        case when year(date_heure) = 2022
            then 1 else 0
        end                                         as flag_crise_nuc_2022,

        -- KPIs CO2
        -- Seuils basés sur les benchmarks européens
        case
            when taux_co2 < 50  then 'Vert'
            when taux_co2 < 100 then 'Orange'
            else                     'Rouge'
        end                                         as indice_vert,

        -- Part bas-carbone = (nucléaire + renouvelables) / consommation
        -- Renouvelables = éolien + solaire + hydraulique + bioénergies
        round(
            (
                coalesce(nucleaire_mw, 0)
                + coalesce(eolien_mw, 0)
                + coalesce(solaire_mw, 0)
                + coalesce(hydraulique_mw, 0)
                + coalesce(bioenergies_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as part_bas_carbone,

        -- Part renouvelables seuls (sans nucléaire)
        round(
            (
                coalesce(eolien_mw, 0)
                + coalesce(solaire_mw, 0)
                + coalesce(hydraulique_mw, 0)
                + coalesce(bioenergies_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as part_renouvelables,

        -- Part fossiles = (gaz + fioul + charbon) / consommation
        round(
            (
                coalesce(gaz_mw, 0)
                + coalesce(fioul_mw, 0)
                + coalesce(charbon_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as part_fossiles

    from source

)

select * from enriched