# source columns summary v7c

## glsxref
- alias: `glsx`
- join_logic:
  - `left join ossbr_2_1 glsxref ref on mas.srseccode = ref.waste_security_code`
- where_like:
  - `qualify row_number() over (partition by mas.srseccode order by mas.srseccode) = 1`
  - `nullif(regexp_replace(trim(mas.srseccode), '\s+', ''), '') is not null`
  - `glsx.srstatus = 'a'`
  - `not exists (select 1 from mfsp where substring(glsx.send_cd, 4, 5) = mfsp.prc_dtl_send_num)`

## mfin
- alias: `edx`

## mfspric
- alias: `mfsp`
- join_logic:
  - `left join ossbr_2_1 glsxref ref on mas.srseccode = ref.waste_security_code`
- where_like:
  - `qualify row_number() over (partition by mas.srseccode order by mas.srseccode) = 1`
  - `nullif(regexp_replace(trim(mas.srseccode), '\s+', ''), '') is not null`
  - `mfsp.srstatus = 'a'`
  - `not exists (select 1 from mfsp where substring(glsx.send_cd, 4, 5) = mfsp.prc_dtl_send_num)`

## ossbr_2_1
- alias: `mas`
- join_logic:
  - `left join ossbr_2_1 glsxref ref on mas.srseccode = ref.waste_security_code`
- where_like:
  - `qualify row_number() over (partition by mas.srseccode order by mas.srseccode) = 1`
  - `nullif(regexp_replace(trim(mas.srseccode), '\s+', ''), '') is not null`
  - `mas.srstatus = 'a'`
  - `not exists (select 1 from mfsp where substring(glsx.send_cd, 4, 5) = mfsp.prc_dtl_send_num)`
- static_assignments:
  - sm_secrty_id ← mas.borid

## tantrum
- alias: `tant`
- static_assignments:
  - tct_issue_in ← 'n'
  - issuer_org_id ← 0
  - secrty_view_cd ← 1342
  - final_offer_dt ← null
  - lifecy_cd ← 114
