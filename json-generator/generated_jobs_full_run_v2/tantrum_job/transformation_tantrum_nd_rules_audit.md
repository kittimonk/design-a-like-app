# Transformation Rules Audit

| Target Column | Raw Transformation (verbatim) | Parsed SQL Expression | Notes |
|---|---|---|---|
| `tgt_column    T_tantrum_0002
tgt_column         tantrumid
Name: 1, dtype: object` | CASE   WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN     CASE       WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM THEN NULL       ELSE         CASE           WHEN SUBSTRING(ref.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(ref.SEND_CD, 7, 2) = '*' THEN CONCAT(230000000000, SUBSTRING(ref.SEND_CD, 4, 3))           WHEN LEN(TRIM(SUBSTRING(ref.SEND_CD, 4, 5))) > 0 THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')), '000')           ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')))         END     END   ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(mas.SRSECCODE), '00'))) END AS tantrum_id | `NULL /* unresolved expression guarded: CASE<br>  WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN<br>    CASE<br>      WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NU... */<br>  END<br>  END` |  |
| `tgt_column    T_tantrum_0002
tgt_column         tantrumid
Name: 0, dtype: object` | CASE   WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN     CASE       WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM THEN NULL       ELSE         CASE           WHEN SUBSTRING(ref.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(ref.SEND_CD, 7, 2) = '*' THEN CONCAT(230000000000, SUBSTRING(ref.SEND_CD, 4, 3))           WHEN LEN(TRIM(SUBSTRING(ref.SEND_CD, 4, 5))) > 0 THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')), '000')           ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')))         END     END   ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(mas.SRSECCODE), '00'))) END AS tantrum_id | `NULL /* unresolved expression guarded: CASE<br>  WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN<br>    CASE<br>      WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NU... */<br>  END<br>  END` |  |
| `tgt_column    T_tantrum_0002
tgt_column         tantrumid
Name: 2, dtype: object` | Set to +00331 (Asset). | `331` | -- NOTE: merged 28 variations for target column 'tgt_column    T_tantrum_0002<br>tgt_column         tantrumid<br>Name: 2, dtype: object' |
