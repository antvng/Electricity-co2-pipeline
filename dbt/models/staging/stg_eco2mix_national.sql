-- Nettoyage et typage des données nationales brutes
-- Sources : RAW_NATIONAL_HISTORIQUE (2012-2025) + RAW_NATIONAL_TEMPS_REEL (2026+)
-- ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_HISTORIQUE
-- ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_TEMPS_REEL

with historique as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_HISTORIQUE

),

temps_reel as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_TEMPS_REEL

),

cleaned_historique as (

    select
        -- Horodatage
        try_to_timestamp("date_heure")              as date_heure,

        -- Consommation et prévisions (MW)
        "consommation"::float                       as consommation_mw,
        "prevision_j1"::float                       as prevision_j1_mw,
        "prevision_j"::float                        as prevision_j_mw,

        -- Mix de production par filière (MW)
        "nucleaire"::float                          as nucleaire_mw,
        "hydraulique"::float                        as hydraulique_mw,
        "eolien"::float                             as eolien_mw,
        "solaire"::float                            as solaire_mw,
        "gaz"::float                                as gaz_mw,
        "fioul"::float                              as fioul_mw,
        "charbon"::float                            as charbon_mw,
        "bioenergies"::float                        as bioenergies_mw,
        "pompage"::float                            as pompage_mw,
        "ech_physiques"::float                      as ech_physiques_mw,

        -- Échanges commerciaux
        "ech_comm_angleterre"::float                as ech_angleterre_mw,
        "ech_comm_espagne"::float                   as ech_espagne_mw,
        "ech_comm_italie"::float                    as ech_italie_mw,
        "ech_comm_suisse"::float                    as ech_suisse_mw,
        "ech_comm_allemagne_belgique"::float        as ech_allemagne_belgique_mw,

        -- KPI central
        "taux_co2"::float                           as taux_co2,

        -- Métadonnées
        "_loaded_at"::timestamp_ntz                 as _loaded_at

    from historique

),

cleaned_temps_reel as (

    select
        -- Horodatage
        DATE_HEURE::timestamp                       as date_heure,

        -- Consommation et prévisions (MW)
        CONSOMMATION::float                         as consommation_mw,
        PREVISION_J1::float                         as prevision_j1_mw,
        PREVISION_J::float                          as prevision_j_mw,

        -- Mix de production par filière (MW)
        NUCLEAIRE::float                            as nucleaire_mw,
        HYDRAULIQUE::float                          as hydraulique_mw,
        EOLIEN::float                               as eolien_mw,
        SOLAIRE::float                              as solaire_mw,
        GAZ::float                                  as gaz_mw,
        FIOUL::float                                as fioul_mw,
        CHARBON::float                              as charbon_mw,
        BIOENERGIES::float                          as bioenergies_mw,
        POMPAGE::float                              as pompage_mw,
        ECH_PHYSIQUES::float                        as ech_physiques_mw,

        -- Échanges commerciaux absents du temps réel → NULL
        NULL::float                                 as ech_angleterre_mw,
        NULL::float                                 as ech_espagne_mw,
        NULL::float                                 as ech_italie_mw,
        NULL::float                                 as ech_suisse_mw,
        NULL::float                                 as ech_allemagne_belgique_mw,

        -- KPI central
        TAUX_CO2::float                             as taux_co2,

        -- Métadonnées
        _LOADED_AT::timestamp_ntz                   as _loaded_at

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
    and consommation_mw > 0

    -- Dédoublonnage sur les changements d'heure
    -- Si même timestamp dans les deux tables, on garde le plus récemment chargé
    qualify row_number() over (
        partition by date_heure
        order by _loaded_at desc
    ) = 1

)

select * from final