-- models/marts/mart_heures_vertes.sql
-- Table de recommandations : top 3 heures vertes par saison
-- PBi Agir selon le CO2
-- Calculé sur tout l'historique
-- Full refresh mensuel

{{
    config(
        materialized='table'
    )
}}

with patterns as (

    -- mart_patterns_co2 déjà calculé
    select *
    from {{ ref('mart_patterns_co2') }}

),

-- Heure de pointe par saison = heure avec le CO2 le plus élevé
pointe_par_saison as (

    select
        saison,
        max(avg_taux_co2)       as co2_heure_pointe,
        min(avg_taux_co2)       as co2_heure_min
    from patterns
    group by saison

),

-- Top 3 heures les plus vertes par saison
top3 as (

    select
        p.saison,
        p.heure                                     as heure_optimale,
        p.avg_taux_co2                              as co2_moyen,
        p.nb_observations,
        p.rang_heure_par_saison                     as rang,

        -- Gain absolu vs heure de pointe (en gCO2/kWh)
        round(
            ps.co2_heure_pointe - p.avg_taux_co2,
        2)                                          as gain_vs_pointe_gco2,

        -- Gain relatif vs heure de pointe (en %)
        round(
            (ps.co2_heure_pointe - p.avg_taux_co2)
            / nullif(ps.co2_heure_pointe, 0) * 100,
        1)                                          as gain_vs_pointe_pct,

        -- CO2 de l'heure de pointe (pour affichage comparatif)
        ps.co2_heure_pointe,

        -- Simulation : émissions pour charger une Renault Zoé (52 kWh)
        -- H Opti vs H Pointe
        round(p.avg_taux_co2 * 52 / 1000, 2)       as emission_zoe_kg_co2,
        round(ps.co2_heure_pointe * 52 / 1000, 2)  as emission_zoe_pointe_kg_co2

    from patterns p
    join pointe_par_saison ps
        on p.saison = ps.saison

    -- Garder uniquement le top 3 par saison
    where p.rang_heure_par_saison <= 3

)

select
    saison,
    rang,
    heure_optimale,
    co2_moyen,
    co2_heure_pointe,
    gain_vs_pointe_gco2,
    gain_vs_pointe_pct,
    emission_zoe_kg_co2,
    emission_zoe_pointe_kg_co2,
    nb_observations
from top3
order by saison, rang