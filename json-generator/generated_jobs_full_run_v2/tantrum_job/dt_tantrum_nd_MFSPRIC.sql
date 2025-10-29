SELECT
    mas.0,
    mas.DTL_SEND_NUM,
    mas.AAW{CZ,DW}/da-cprd/aaw/stage/mbs_gls
FROM MFSPRIC mas

WHERE
  -- Business Rule Block #1
  -- NOTE: Evaluate rule -> reject the record if duplicate mas.SRSECCODE found and
  -- NOTE: Evaluate rule -> reject the record and
  -- NOTE: Exclusion rule -> 3) If mas.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required)
  mas.SRSTATUS = 'A'
