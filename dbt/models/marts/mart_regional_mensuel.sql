-- models/marts/mart_regional_mensuel.sql
-- Agrégat mensuel par région — alimente la carte choroplèthe Power BI
-- Modèle TABLE avec full refresh

{{
    config(
        materialized='table'
    )
}}

with source as (

    select *
    from {{ ref('int_regional_enrichi') }}

),

aggregated as (

    select
        -- Dimensions de groupement
        annee,
        mois,
        libelle_region,
        code_insee_region,

        -- Métriques de production (moyennes mensuelles en MW)
        round(avg(consommation_mw), 2)              as avg_consommation_mw,
        round(avg(production_totale_mw), 2)         as avg_production_mw,
        round(avg(nucleaire_mw), 2)                 as avg_nucleaire_mw,
        round(avg(eolien_mw), 2)                    as avg_eolien_mw,
        round(avg(solaire_mw), 2)                   as avg_solaire_mw,
        round(avg(hydraulique_mw), 2)               as avg_hydraulique_mw,
        round(avg(thermique_mw), 2)                 as avg_thermique_mw,
        round(avg(bioenergies_mw), 2)               as avg_bioenergies_mw,

        -- KPIs mensuels
        round(avg(autosuffisance), 4)               as autosuffisance_moyenne,
        round(avg(part_renouvelables), 4)           as part_renouvelables_moyenne,
        round(avg(part_bas_carbone), 4)             as part_bas_carbone_moyenne,
        round(avg(tco_renouvelables), 2)            as tco_renouvelables_moyen,

        -- Flags agrégés
        max(flag_covid)                             as flag_covid,
        max(flag_crise_nuc_2022)                    as flag_crise_nuc_2022,

        -- Nombre d'observations pour fiabilité statistique
        count(*)                                    as nb_observations

    from source
    group by
        annee,
        mois,
        libelle_region,
        code_insee_region

)

select * from aggregated
order by annee, mois, libelle_region