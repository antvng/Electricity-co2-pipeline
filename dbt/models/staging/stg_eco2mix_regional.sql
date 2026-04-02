-- Nettoyage et typage des données régionales brutes
-- ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_HISTORIQUE

with source as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_HISTORIQUE

),

cleaned as (

    select
        -- Identifiant région
        "code_insee_region"::varchar                as code_insee_region,
        "libelle_region"::varchar                   as libelle_region,

        -- Horodatage
        try_to_timestamp("date_heure")              as date_heure,

        -- Consommation (MW)
        try_to_double("consommation")               as consommation_mw,

        -- Mix de production par filière (MW)
        try_to_double("thermique")                  as thermique_mw,
        try_to_double("nucleaire")                  as nucleaire_mw,
        try_to_double("eolien")                     as eolien_mw,
        try_to_double("solaire")                    as solaire_mw,
        try_to_double("hydraulique")                as hydraulique_mw,
        try_to_double("pompage")                    as pompage_mw,
        try_to_double("bioenergies")                as bioenergies_mw,
        try_to_double("ech_physiques")              as ech_physiques_mw,
        try_to_double("eolien_terrestre")           as eolien_terrestre_mw,
        try_to_double("eolien_offshore")            as eolien_offshore_mw,

        -- Taux de couverture par filière (%)
        try_to_double("tco_thermique")              as tco_thermique,
        try_to_double("tco_nucleaire")              as tco_nucleaire,
        try_to_double("tco_eolien")                 as tco_eolien,
        try_to_double("tco_solaire")                as tco_solaire,
        try_to_double("tco_hydraulique")            as tco_hydraulique,
        try_to_double("tco_bioenergies")            as tco_bioenergies,

        -- Métadonnées
        "_loaded_at"                                as _loaded_at

    from source

),

final as (

    select *
    from cleaned
    where date_heure is not null
    and consommation_mw is not null
    and libelle_region is not null

    -- Dédoublonnage sur les changements d'heure
    -- On garde la dernière ligne seulement pour chaque timestamp × région
    qualify row_number() over (
        partition by date_heure, libelle_region
        order by _loaded_at desc
    ) = 1

)

select * from final