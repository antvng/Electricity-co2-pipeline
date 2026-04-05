-- Nettoyage et typage des données régionales brutes
-- Sources : RAW_REGIONAL_HISTORIQUE (2013-2025) + RAW_REGIONAL_TEMPS_REEL (2026+)
-- ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_HISTORIQUE
-- ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_TEMPS_REEL

with historique as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_HISTORIQUE

),

temps_reel as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_REGIONAL_TEMPS_REEL

),

cleaned_historique as (

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
        "_loaded_at"::timestamp_ntz                 as _loaded_at

    from historique

),

cleaned_temps_reel as (

    select
        -- Identifiant région
        code_insee_region::varchar                  as code_insee_region,
        libelle_region::varchar                     as libelle_region,

        -- Horodatage
        date_heure::timestamp                       as date_heure,

        -- Consommation (MW)
        consommation::float                         as consommation_mw,

        -- Mix de production par filière (MW)
        thermique::float                            as thermique_mw,
        nucleaire::float                            as nucleaire_mw,
        eolien::float                               as eolien_mw,
        solaire::float                              as solaire_mw,
        hydraulique::float                          as hydraulique_mw,
        pompage::float                              as pompage_mw,
        bioenergies::float                          as bioenergies_mw,
        ech_physiques::float                        as ech_physiques_mw,

        -- Absent du temps réel → NULL
        NULL::float                                 as eolien_terrestre_mw,
        NULL::float                                 as eolien_offshore_mw,

        -- Taux de couverture par filière (%)
        tco_thermique::float                        as tco_thermique,
        tco_nucleaire::float                        as tco_nucleaire,
        tco_eolien::float                           as tco_eolien,
        tco_solaire::float                          as tco_solaire,
        tco_hydraulique::float                      as tco_hydraulique,
        tco_bioenergies::float                      as tco_bioenergies,

        -- Métadonnées
        _loaded_at::timestamp_ntz                   as _loaded_at

    from temps_reel

),

source as (

    select * from cleaned_historique
    union all
    select * from cleaned_temps_reel

),

final as (

    select *
    from source
    where date_heure is not null
    and consommation_mw is not null
    and libelle_region is not null

    -- Dédoublonnage sur les changements d'heure
    -- Si même timestamp × région dans les deux tables, on garde le plus récent
    qualify row_number() over (
        partition by date_heure, libelle_region
        order by _loaded_at desc
    ) = 1

)

select * from final