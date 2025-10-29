SELECT
    mas.AAW{CZ,DW}/da-cprd/aaw/stage/mbs_gls,
    mas.tantrum_family_CD
FROM tantrum mas
LEFT JOIN tantrum tan ON mas.SRSECCODE = tantrum.SRSECCODE
