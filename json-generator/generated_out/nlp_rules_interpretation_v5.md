# NLP Parsing Report (v5)

## Source: glsxref
- Alias: `ref`
- Known columns: SEND_CD, SEND_DESC, WASTE_SECURITY_CD
- Referenced columns: WASTE_SECURITY_CODE, FUND_COMPANY, FUND_NUMBER, SEND_CD, sm_SECURITY_CODE, FUND_DESC
- Candidate WHERE predicates:
  - `log an exception 3) If mas.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required) 4) If security from GLS daily security master is found on mutual fund cross reference file (ref.WASTE_SECURITY_CODE = mas.SRSECCODE)`
  - `the mutual fund is a SB Fund (1st 3 bytes of ref.FUND_COMPANY = 'SBB')    then match to the mutual fund instrument price file - ref.FUND_NUMBER = MFSPRIC.DTL_SEND_NUM.    If a match found then don't extract the record (no exception logging required - instrument already extracted by MFS extract).  Note: 1,2,3 -> Need to get the SRSECCODE`
- CASE-like expressions:
```sql
CASE   WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN     CASE       WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM THEN NULL       ELSE         CASE           WHEN SUBSTRING(ref.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(ref.SEND_CD, 7, 2) = '*' THEN CONCAT(230000000000, SUBSTRING(ref.SEND_CD, 4, 3))           WHEN LEN(TRIM(SUBSTRING(ref.SEND_CD, 4, 5))) > 0 THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')), '000')           ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')))         END     END   ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(mas.SRSECCODE), '00'))) END AS tantrum_id
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN RTRIM(ref.FUND_DESC) ELSE RTRIM(mas.SRSHSBESE) END AS tantrum_na
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) IN ('SBM', 'SBG', 'SBS','SBB', 'SBC') THEN 'Y' WHEN LEFT(mas.SRSHSBESE, 2) = 'SB' THEN 'Y' ELSE 'N' END AS tantrum_issued_in
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) IN ('SBM', 'SBG', 'SBS','SBB', 'SBC') THEN 'Y' WHEN LEFT(mas.SRSHSBESE, 2) = 'SB' THEN 'Y' ELSE 'N' END AS tantrum_issued_in
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN ref.SEND_CD ELSE '00000000' END AS mu_fund_ID
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN ref.SEND_CD ELSE '00000000' END AS mu_fund_ID
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) = 'SBB' THEN CASE WHEN MFIN.MFIN_SEND_NUMBER IS NOT NULL THEN CASE WHEN MFIN.MFIN_ASSET_ORDER = 1 THEN '+08830' -- MMK /n WHEN MFIN.MFIN_ASSET_ORDER = 2 THEN '+08831' -- PMMK /n WHEN MFIN.MFIN_ASSET_ORDER = 3 THEN '+08832' -- ING /n WHEN MFIN.MFIN_ASSET_ORDER = 4 THEN '+08833' -- BAL /n WHEN MFIN.MFIN_ASSET_ORDER = 5 THEN '+08834' -- INX /n WHEN MFIN.MFIN_ASSET_ORDER = 6 THEN '+08835' -- CDNEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 7 THEN '+08836' -- USEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 8 THEN '+08837' -- SEC /n WHEN MFIN.MFIN_ASSET_ORDER = 9 THEN '+08838' -- GLO /n WHEN MFIN.MFIN_ASSET_ORDER = 10 THEN '+08839' -- MAPM /n WHEN MFIN.MFIN_ASSET_ORDER = 11 THEN '+08840' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 12 THEN '+08841' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 13 THEN '+08842' -- GIC /n WHEN MFIN.MFIN_ASSET_ORDER = 14 THEN '+12553' -- COMFORT /n WHEN MFIN.MFIN_ASSET_ORDER = 15 THEN '+12554' -- ADVANTAGE /n WHEN MFIN.MFIN_ASSET_ORDER = 16 THEN '+12559' -- CORPORATE /n ELSE '-00004' -- Invalid /n END ELSE '-00001' -- Not Applicable /n END ELSE '-00001' -- Not Applicable /n END AS mu_fund_FAMILY_CD
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) = 'SBB' THEN CASE WHEN MFIN.MFIN_SEND_NUMBER IS NOT NULL THEN CASE WHEN MFIN.MFIN_ASSET_ORDER = 1 THEN '+08830' -- MMK /n WHEN MFIN.MFIN_ASSET_ORDER = 2 THEN '+08831' -- PMMK /n WHEN MFIN.MFIN_ASSET_ORDER = 3 THEN '+08832' -- ING /n WHEN MFIN.MFIN_ASSET_ORDER = 4 THEN '+08833' -- BAL /n WHEN MFIN.MFIN_ASSET_ORDER = 5 THEN '+08834' -- INX /n WHEN MFIN.MFIN_ASSET_ORDER = 6 THEN '+08835' -- CDNEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 7 THEN '+08836' -- USEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 8 THEN '+08837' -- SEC /n WHEN MFIN.MFIN_ASSET_ORDER = 9 THEN '+08838' -- GLO /n WHEN MFIN.MFIN_ASSET_ORDER = 10 THEN '+08839' -- MAPM /n WHEN MFIN.MFIN_ASSET_ORDER = 11 THEN '+08840' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 12 THEN '+08841' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 13 THEN '+08842' -- GIC /n WHEN MFIN.MFIN_ASSET_ORDER = 14 THEN '+12553' -- COMFORT /n WHEN MFIN.MFIN_ASSET_ORDER = 15 THEN '+12554' -- ADVANTAGE /n WHEN MFIN.MFIN_ASSET_ORDER = 16 THEN '+12559' -- CORPORATE /n ELSE '-00004' -- Invalid /n END ELSE '-00001' -- Not Applicable /n END ELSE '-00001' -- Not Applicable /n END AS mu_fund_FAMILY_CD
```

## Source: mfin
- Alias: `edx`
- Known columns: MFIN_ASSET_ORDER, MFIN_SEND_NUMBER
- Referenced columns: MFIN_SEND_NUMBER, MFIN_ASSET_ORDER
- CASE-like expressions:
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) = 'SBB' THEN CASE WHEN MFIN.MFIN_SEND_NUMBER IS NOT NULL THEN CASE WHEN MFIN.MFIN_ASSET_ORDER = 1 THEN '+08830' -- MMK /n WHEN MFIN.MFIN_ASSET_ORDER = 2 THEN '+08831' -- PMMK /n WHEN MFIN.MFIN_ASSET_ORDER = 3 THEN '+08832' -- ING /n WHEN MFIN.MFIN_ASSET_ORDER = 4 THEN '+08833' -- BAL /n WHEN MFIN.MFIN_ASSET_ORDER = 5 THEN '+08834' -- INX /n WHEN MFIN.MFIN_ASSET_ORDER = 6 THEN '+08835' -- CDNEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 7 THEN '+08836' -- USEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 8 THEN '+08837' -- SEC /n WHEN MFIN.MFIN_ASSET_ORDER = 9 THEN '+08838' -- GLO /n WHEN MFIN.MFIN_ASSET_ORDER = 10 THEN '+08839' -- MAPM /n WHEN MFIN.MFIN_ASSET_ORDER = 11 THEN '+08840' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 12 THEN '+08841' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 13 THEN '+08842' -- GIC /n WHEN MFIN.MFIN_ASSET_ORDER = 14 THEN '+12553' -- COMFORT /n WHEN MFIN.MFIN_ASSET_ORDER = 15 THEN '+12554' -- ADVANTAGE /n WHEN MFIN.MFIN_ASSET_ORDER = 16 THEN '+12559' -- CORPORATE /n ELSE '-00004' -- Invalid /n END ELSE '-00001' -- Not Applicable /n END ELSE '-00001' -- Not Applicable /n END AS mu_fund_FAMILY_CD
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) = 'SBB' THEN CASE WHEN MFIN.MFIN_SEND_NUMBER IS NOT NULL THEN CASE WHEN MFIN.MFIN_ASSET_ORDER = 1 THEN '+08830' -- MMK /n WHEN MFIN.MFIN_ASSET_ORDER = 2 THEN '+08831' -- PMMK /n WHEN MFIN.MFIN_ASSET_ORDER = 3 THEN '+08832' -- ING /n WHEN MFIN.MFIN_ASSET_ORDER = 4 THEN '+08833' -- BAL /n WHEN MFIN.MFIN_ASSET_ORDER = 5 THEN '+08834' -- INX /n WHEN MFIN.MFIN_ASSET_ORDER = 6 THEN '+08835' -- CDNEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 7 THEN '+08836' -- USEQ /n WHEN MFIN.MFIN_ASSET_ORDER = 8 THEN '+08837' -- SEC /n WHEN MFIN.MFIN_ASSET_ORDER = 9 THEN '+08838' -- GLO /n WHEN MFIN.MFIN_ASSET_ORDER = 10 THEN '+08839' -- MAPM /n WHEN MFIN.MFIN_ASSET_ORDER = 11 THEN '+08840' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 12 THEN '+08841' -- MAP /n WHEN MFIN.MFIN_ASSET_ORDER = 13 THEN '+08842' -- GIC /n WHEN MFIN.MFIN_ASSET_ORDER = 14 THEN '+12553' -- COMFORT /n WHEN MFIN.MFIN_ASSET_ORDER = 15 THEN '+12554' -- ADVANTAGE /n WHEN MFIN.MFIN_ASSET_ORDER = 16 THEN '+12559' -- CORPORATE /n ELSE '-00004' -- Invalid /n END ELSE '-00001' -- Not Applicable /n END ELSE '-00001' -- Not Applicable /n END AS mu_fund_FAMILY_CD
```

## Source: mfspric
- Alias: ``
- Known columns: DTL_SEND_NUM
- Referenced columns: DTL_SEND_NUM, PRC_DTL_SEND_NUM
- Candidate WHERE predicates:
  - `1) reject the record if duplicate mas.SRSECCODE found`
  - `log an exception 2) If mas.SRSECCODE is all spaces    reject the record`
  - `log an exception 3) If mas.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required) 4) If security from GLS daily security master is found on mutual fund cross reference file (ref.WASTE_SECURITY_CODE = mas.SRSECCODE)`
  - `the mutual fund is a SB Fund (1st 3 bytes of ref.FUND_COMPANY = 'SBB')    then match to the mutual fund instrument price file - ref.FUND_NUMBER = MFSPRIC.DTL_SEND_NUM.    If a match found then don't extract the record (no exception logging required - instrument already extracted by MFS extract).  Note: 1,2,3 -> Need to get the SRSECCODE`
- CASE-like expressions:
```sql
CASE   WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN     CASE       WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM THEN NULL       ELSE         CASE           WHEN SUBSTRING(ref.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(ref.SEND_CD, 7, 2) = '*' THEN CONCAT(230000000000, SUBSTRING(ref.SEND_CD, 4, 3))           WHEN LEN(TRIM(SUBSTRING(ref.SEND_CD, 4, 5))) > 0 THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')), '000')           ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')))         END     END   ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(mas.SRSECCODE), '00'))) END AS tantrum_id
```

## Source: ossbr_2_1
- Alias: `mas`
- Known columns: SRPFREQ, borclass, borid, curr_type_CD, cusip, foreigndate, interestrate, maturitydate, shortdescription, srctype
- Referenced columns: SRSECCODE, SRSTATUS, SRSHSBESE, SRSECTYPE, SRCURRCODE, secrty_curncy_id, SBDSBDATE, SRPREQ, SRPMTRATE, SRCUSIPNBR, SRSECCLAS
- Candidate WHERE predicates:
  - `1) reject the record if duplicate mas.SRSECCODE found`
  - `log an exception 2) If mas.SRSECCODE is all spaces    reject the record`
  - `log an exception 3) If mas.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required) 4) If security from GLS daily security master is found on mutual fund cross reference file (ref.WASTE_SECURITY_CODE = mas.SRSECCODE)`
  - `CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN RTRIM(ref.FUND_DESC) ELSE RTRIM(mas.SRSHSBESE)`
  - `CASE WHEN mas.SRCURRCODE IN ('BEF', 'DEM', 'FRF', 'GRD', 'TEP', 'ITL','NLG','PTE') THEN '+00618' ELSE CASE WHEN mas.secrty_curncy_id IN (-3,-4) THEN '+00616' ELSE COALESCE(get_stndrd_id('EDW','EDW_CURNCY_CD', 'N', mas.secrty_curncy_id), '+00616') END END AS CURNCY_CD FROM ossbr_2_1 mas`
  - `CASE WHEN TRY_CAST(mas.SBDSBDATE AS DATE) IS NOT NULL`
  - `mas.SBDSBDATE <> '0001-01-01' THEN mas.SBDSBDATE ELSE NULL END AS FIRST_OFFER_DT FROM ossbr_2_1 mas`
  - `CASE WHEN TRY_CAST(mas.SRPMTRATE AS FLOAT) IS NOT NULL`
  - `mas.SRPMTRATE > 0`
  - `mas.SRPMTRATE <= 999.99999 THEN mas.SRPMTRATE ELSE NULL END AS INT_RT FROM ossbr_2_1 mas`
  - `CASE WHEN mas.SRSECTYPE = '210' THEN 'N' ELSE 'Y' END AS ERLCSH_ELIGBL_IN FROM ossbr_2_1 mas`
  - `CASE WHEN RTRIM(mas.SRCUSIPNBR) = '1' THEN '000000000' ELSE mas.SRCUSIPNBR END AS CUSIP_ID FROM ossbr_2_1 mas`
  - `CASE WHEN TRY_CAST(mas.SRSECTYPE AS INT) IS NOT NULL THEN mas.SRSECTYPE ELSE '+00000' END AS sm.SECRTY_TYPE_ID FROM ossbr_2_1 mas`
  - `CASE WHEN TRY_CAST(mas.SRSECCLAS AS INT) IS NOT NULL THEN mas.SRSECCLAS ELSE '0' END AS SECRTY_CLASS_ID FROM ossbr_2_1 mas`
- CASE-like expressions:
```sql
CASE   WHEN LEFT(ref.SEND_CD, 3) = 'SBB' THEN     CASE       WHEN SUBSTRING(ref.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM THEN NULL       ELSE         CASE           WHEN SUBSTRING(ref.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(ref.SEND_CD, 7, 2) = '*' THEN CONCAT(230000000000, SUBSTRING(ref.SEND_CD, 4, 3))           WHEN LEN(TRIM(SUBSTRING(ref.SEND_CD, 4, 5))) > 0 THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')), '000')           ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(ref.SEND_CD, 4, 1)), '00')))         END     END   ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(mas.SRSECCODE), '00'))) END AS tantrum_id
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN RTRIM(ref.FUND_DESC) ELSE RTRIM(mas.SRSHSBESE)
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL THEN CASE WHEN mas.SRSECTYPE IN ('380','331') THEN '+01313' ELSE '+01316' END ELSE CASE WHEN mas.SRSECTYPE = '220' THEN '+01312' WHEN mas.SRSECTYPE IN ('315','600','520','420','310','330','510') THEN '+01314'  WHEN mas.SRSECTYPE IN ('551','553','555','559','567','550','556','557','570','581','554','558','568','569','573','562','530') THEN '+01315' WHEN mas.SRSECTYPE IN ('210','230','240') THEN '+01309' WHEN mas.SRSECTYPE IN ('263','265','275','260','261','270','262','276','290','295') THEN '+01310' ELSE '+01316'  END END AS tantrum_family_cd
```
```sql
CASE WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) IN ('SBM', 'SBG', 'SBS','SBB', 'SBC') THEN 'Y' WHEN LEFT(mas.SRSHSBESE, 2) = 'SB' THEN 'Y' ELSE 'N' END AS tantrum_issued_in
```
```sql
CASE WHEN mas.SRCURRCODE IN ('BEF', 'DEM', 'FRF', 'GRD', 'TEP', 'ITL','NLG','PTE') THEN '+00618' ELSE CASE WHEN mas.secrty_curncy_id IN (-3,-4) THEN '+00616' ELSE COALESCE(get_stndrd_id('EDW','EDW_CURNCY_CD', 'N', mas.secrty_curncy_id), '+00616') END END AS CURNCY_CD
```
```sql
CASE WHEN TRY_CAST(mas.SBDSBDATE AS DATE) IS NOT NULL AND mas.SBDSBDATE <> '0001-01-01' THEN mas.SBDSBDATE ELSE NULL END AS FIRST_OFFER_DT
```
```sql
CASE -- Check if tantrum_FAMLY_CD is +01309 (MMK) or +01310 (FIX)   WHEN tantrum.INSTR_FAMILY_CD IN ('+01309', '+01310') THEN COALESCE(get_stndrd_id('EDW', 'EDW_INT_FREQ_CD', 'N', mas.SRPREQ),'-00001') -- Default to Not Applicable if no standard mapping is found   ELSE '-00001' -- Not Applicable   END AS INT_FREQ_CD
```
```sql
CASE -- Check if tantrum_FAMLY_CD is +01309 (MMK) or +01310 (FIX)   WHEN tantrum.INSTR_FAMILY_CD IN ('+01309', '+01310') THEN COALESCE(get_stndrd_id('EDW', 'EDW_INT_FREQ_CD', 'N', mas.SRPREQ),'-00001') -- Default to Not Applicable if no standard mapping is found   ELSE '-00001' -- Not Applicable   END AS INT_FREQ_CD
```
```sql
CASE WHEN TRY_CAST(mas.SRPMTRATE AS FLOAT) IS NOT NULL AND mas.SRPMTRATE > 0 AND mas.SRPMTRATE <= 999.99999 THEN mas.SRPMTRATE ELSE NULL END AS INT_RT
```
```sql
CASE WHEN mas.SRSECTYPE = '210' THEN 'N' ELSE 'Y' END AS ERLCSH_ELIGBL_IN
```
```sql
CASE WHEN RTRIM(mas.SRCUSIPNBR) = '1' THEN '000000000' ELSE mas.SRCUSIPNBR END AS CUSIP_ID
```
```sql
CASE WHEN TRY_CAST(mas.SRSECTYPE AS INT) IS NOT NULL THEN mas.SRSECTYPE ELSE '+00000' END AS sm.SECRTY_TYPE_ID
```
```sql
CASE WHEN TRY_CAST(mas.SRSECCLAS AS INT) IS NOT NULL THEN mas.SRSECCLAS ELSE '0' END AS SECRTY_CLASS_ID
```

## Source: tantrum
- Alias: ``
- Known columns: tantrum_family_CD
- Referenced columns: INSTR_FAMILY_CD, SRSECCODE
- Candidate WHERE predicates:
  - `Set to etl.effective.start.date in the fromat yyyy-mm-dd`
- CASE-like expressions:
```sql
CASE -- Check if tantrum_FAMLY_CD is +01309 (MMK) or +01310 (FIX)   WHEN tantrum.INSTR_FAMILY_CD IN ('+01309', '+01310') THEN COALESCE(get_stndrd_id('EDW', 'EDW_INT_FREQ_CD', 'N', mas.SRPREQ),'-00001') -- Default to Not Applicable if no standard mapping is found   ELSE '-00001' -- Not Applicable   END AS INT_FREQ_CD
```
