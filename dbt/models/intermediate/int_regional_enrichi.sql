-- Enrichissement des données régionales avec dimensions temporelles et KPIs
-- stg_eco2mix_regional

with source as (

    select *
    from {{ ref('stg_eco2mix_regional') }}

),

enriched as (

    select
        -- Colonnes de base
        date_heure,
        code_insee_region,
        libelle_region,
        consommation_mw,
        thermique_mw,
        nucleaire_mw,
        eolien_mw,
        solaire_mw,
        hydraulique_mw,
        pompage_mw,
        bioenergies_mw,
        ech_physiques_mw,
        eolien_terrestre_mw,
        eolien_offshore_mw,
        tco_thermique,
        tco_nucleaire,
        tco_eolien,
        tco_solaire,
        tco_hydraulique,
        tco_bioenergies,
        _loaded_at,

        -- Dimensions temporelles (même logique que le national)
        hour(date_heure)                            as heure,
        month(date_heure)                           as mois,
        year(date_heure)                            as annee,

        case dayofweek(date_heure)
            when 0 then 'Dimanche'
            when 1 then 'Lundi'
            when 2 then 'Mardi'
            when 3 then 'Mercredi'
            when 4 then 'Jeudi'
            when 5 then 'Vendredi'
            when 6 then 'Samedi'
        end                                         as jour_semaine,

        case when dayofweek(date_heure) in (0, 6)
            then 1 else 0
        end                                         as flag_weekend,

        case
            when month(date_heure) in (12, 1, 2)  then 'Hiver'
            when month(date_heure) in (3, 4, 5)   then 'Printemps'
            when month(date_heure) in (6, 7, 8)   then 'Été'
            when month(date_heure) in (9, 10, 11) then 'Automne'
        end                                         as saison,

        -- Flags événements historiques (même que national)
        case when date_heure between '2020-03-17' and '2020-05-11'
            then 1 else 0
        end                                         as flag_covid,

        case when year(date_heure) = 2022
            then 1 else 0
        end                                         as flag_crise_nuc_2022,

        -- Production totale toutes filières
        coalesce(thermique_mw, 0)
        + coalesce(nucleaire_mw, 0)
        + coalesce(eolien_mw, 0)
        + coalesce(solaire_mw, 0)
        + coalesce(hydraulique_mw, 0)
        + coalesce(bioenergies_mw, 0)               as production_totale_mw,

        -- Taux d'autosuffisance = production locale / consommation
        -- > 1 = région exportatrice, < 1 = région importatrice
        round(
            (
                coalesce(thermique_mw, 0)
                + coalesce(nucleaire_mw, 0)
                + coalesce(eolien_mw, 0)
                + coalesce(solaire_mw, 0)
                + coalesce(hydraulique_mw, 0)
                + coalesce(bioenergies_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as autosuffisance,

        -- Flag région importatrice (solde échanges négatif)
        case when ech_physiques_mw < 0
            then 1 else 0
        end                                         as flag_importatrice,

        -- Part renouvelables = (éolien + solaire + hydraulique + bioénergies) / conso
        round(
            (
                coalesce(eolien_mw, 0)
                + coalesce(solaire_mw, 0)
                + coalesce(hydraulique_mw, 0)
                + coalesce(bioenergies_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as part_renouvelables,

        -- Part bas-carbone = (nucléaire + renouvelables) / conso
        round(
            (
                coalesce(nucleaire_mw, 0)
                + coalesce(eolien_mw, 0)
                + coalesce(solaire_mw, 0)
                + coalesce(hydraulique_mw, 0)
                + coalesce(bioenergies_mw, 0)
            ) / nullif(consommation_mw, 0),
        4)                                          as part_bas_carbone,

        -- TCO renouvelables composite
        -- Somme des taux de couverture renouvelables disponibles
        round(
            coalesce(tco_eolien, 0)
            + coalesce(tco_solaire, 0)
            + coalesce(tco_hydraulique, 0)
            + coalesce(tco_bioenergies, 0),
        2)                                          as tco_renouvelables

    from source

)

select * from enriched