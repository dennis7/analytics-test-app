MODEL (
    name waci.prep_private_securities,
    kind FULL,
    audits (assert_waci_prep_private_securities_not_null),
    description 'Private positions mapped to internal security identifiers via fund ID'
);

@SHARED_PREP_PRIVATE_SECURITIES()
