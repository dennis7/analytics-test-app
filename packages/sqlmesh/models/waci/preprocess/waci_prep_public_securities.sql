MODEL (
    name waci.prep_public_securities,
    kind FULL,
    audits (assert_waci_prep_public_securities_not_null),
    description 'Public positions mapped to internal security identifiers via ISIN'
);

@SHARED_PREP_PUBLIC_SECURITIES()
