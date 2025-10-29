SELECT
    mas.*
FROM GLSXREF mas
LEFT JOIN GLSXREF ref1 ON mas.SRSECCODE = ref.sm_SECURITY_CODE
LEFT JOIN GLSXREF ref2 ON mas.SRSECCODE = ref.sm_SECURITY_CODE LEFT JOIN MFIN edx ON SUBSTRING (ref.SEND_CD, 4, 5) = edx.MFIN_SEND_NUMBER

WHERE
  -- Business Rule Block #1
  -- NOTE: Evaluate rule -> reject the record if duplicate mas.SRSECCODE found and
  -- NOTE: Evaluate rule -> reject the record and
  -- NOTE: Exclusion rule -> 3) If mas.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required)
  mas.SRSTATUS = 'A'
