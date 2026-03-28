-- models/marts/mart_patterns_co2.sql
-- Agrégats CO2 par heure × saison × jour_semaine
-- HeatMap
-- Modèle TABLE avec full refresh

{{
    config(
        materialized='table'
    )
}}

with source as (

    select *
    from {{ ref('int_national_enrichi') }}

    -- On exclut 2022 et 2020 des patterns "normaux" pour ne pas biaiser les moyennes histo
    -- On les analysera sous flags
    where flag_crise_nuc_2022 = 0
    and flag_covid = 0

),

aggregated as (

    select
        -- Dimensions de groupement
        heure,
        saison,
        jour_semaine,
        flag_weekend,

        -- Métriques CO2
        -- Moyenne sur tout l'historique
        round(avg(taux_co2), 2)                     as avg_taux_co2,
        round(min(taux_co2), 2)                     as min_taux_co2,
        round(max(taux_co2), 2)                     as max_taux_co2,
        round(stddev(taux_co2), 2)                  as stddev_taux_co2,

        -- Nombre d'observations
        count(*)                                    as nb_observations,

        -- Métriques mix énergétique
        round(avg(part_bas_carbone), 4)             as avg_part_bas_carbone,
        round(avg(part_renouvelables), 4)           as avg_part_renouvelables,
        round(avg(part_fossiles), 4)                as avg_part_fossiles,
        round(avg(consommation_mw), 2)              as avg_consommation_mw,
        round(avg(nucleaire_mw), 2)                 as avg_nucleaire_mw,
        round(avg(eolien_mw), 2)                    as avg_eolien_mw,
        round(avg(solaire_mw), 2)                   as avg_solaire_mw,
        round(avg(gaz_mw), 2)                       as avg_gaz_mw

    from source
    group by
        heure,
        saison,
        jour_semaine,
        flag_weekend

),

ranked as (

    select
        *,

        -- Rang de chaque heure par saison (croissant h la plus eco)
        -- PBi Top 3?
        rank() over (
            partition by saison
            order by avg_taux_co2 asc
        )                                           as rang_heure_par_saison,

        -- Rang global 
        rank() over (
            order by avg_taux_co2 asc
        )                                           as rang_global

    from aggregated

)

select * from ranked