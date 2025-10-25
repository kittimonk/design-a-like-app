WITH
src AS (SELECT * FROM GLSXREF src),
src1 AS (SELECT * FROM MFIN src1),
src2 AS (SELECT * FROM MFSPRIC src2),
ossbr AS (SELECT * FROM ossbr_2_1 ossbr),
src3 AS (SELECT * FROM tantrum src3),
step1 AS (
  SELECT ossbr.*
  FROM ossbr_2_1 ossbr
  LEFT JOIN GLSXREF ref ON ossbr.SRSECCODE = ref.WASTE_SECURITY_CODE

WHERE
  -- Business Rule Block #1
  -- TODO: Duplicates: enforce ROW_NUMBER() OVER (PARTITION BY mas.SRSECCODE ORDER BY <choose>) = 1
  -- NOTE: Evaluate rule -> reject the record if duplicate ossbr_2_1.SRSECCODE found and
  -- NOTE: Evaluate rule -> reject the record and
  -- NOTE: Exclusion rule -> 3) If ossbr_2_1.SRSTATUS <> 'A' (i.e. not active) exclude the record (no exception logging required)
  TRIM(mas.SRSECCODE) <> '' AND mas.SRSTATUS = 'A'
)
SELECT
    CAST(331 AS BIGINT) AS asset_liblty_CD,
    CAST(-2 AS BIGINT) AS compnd_int_CD,
    CASE
   
    WHEN mas.SRCURRCODE IN ('BEF', 'DEM', 'FRF', 'GRD', 'TEP', 'ITL','NLG','PTE') 
      THEN '+00618' 
    ELSE CASE
   
    WHEN mas.secrty_curncy_id IN (-3,-4) 
      THEN '+00616' 
    ELSE COALESCE(get stndrd_id('EDW','EDW_CURNCY_CD', 'N', mas.secrty_curncy_id), '+00616') 
  END 
  END AS CURNCY_CD
    -- Source context preserved: FROM ossbr_2_1 mas,
    CASE
   
    WHEN RTRIM(mas.SRCUSIPNBR)=  
      THEN '000000000' 
    ELSE mas.SRCUSIPNBR 
  END AS CUSIP_ID
    -- Source context preserved: FROM ossbr_2_1 mas,
    CASE
   
    WHEN mas.SRSECTYPE = '210' 
      THEN 'N' 
    ELSE 'Y' 
  END AS ERLCSH_ELIGBL_IN
    -- Source context preserved: FROM ossbr_2_1 mas,
    CAST(NULL AS DATE) AS final_offer_dt,
    CASE
   
    WHEN TRY_CAST(mas.SBDSBDATE AS DATE) IS NOT NULL AND mas.SBDSBDATE <> '0001-01-01' 
      THEN mas.SBDSBDATE 
    ELSE NULL 
  END AS FIRST_OFFER_DT
    -- Source context preserved: FROM ossbr_2_1 mas,
    CAST('A' AS BIGINT) AS fis_paper_type_CD,
    CAST(0 AS BIGINT) AS fis_secrty_id,
    -- NOTE: merged 3 duplicate definitions for target column 'int_freq_CD'
    CASE
   -- Check if tantrum_FAMLY_CD is +01309 (MMK) or +01310 (FIX) 
 
    WHEN tantrum.INSTR_FAMILY_CD IN ('+01309', '+01310') 
      THEN COALESCE(get_stndrd _id('EDW', 'EDW_INT_FREQ_CD', 'N', mas.SRPREQ),'-00001' -- Default to Not Applicable if no standard mapping is found 
 
    ELSE '-00001' -- Not Applicable 
 
  END AS INT_FREQ_CD
    -- Source context preserved: FROM ossbr_2_1 mas LEFT JOIN tantrum tantrum ON mas.SRSECCODE = tantrum.SRSECCODE,
    CAST(0 AS BIGINT) AS int_rate_id,
    CAST(-2 AS BIGINT) AS int_relatn_CD,
    CASE
   
    WHEN TRY_CAST(mas.SRPMTRATE AS FLAOT) IS NOT NULL AND mas.SRPMTRATE > 0 AND mas.SRPMTRATE <= 999.99999 
      THEN mas.SRPMTRATE 
    ELSE NULL 
  END AS INT_RT
    -- Source context preserved: FROM ossbr_2_1 mas,
    CAST(0 AS BIGINT) AS issuer_org_id,
    TO_DATE('"""${etl.effective.start.date}"""', 'yyyyMMddHHmmss') AS last_change_dt,
    CAST(114 AS BIGINT) AS lifecy_CD,
    CURRENT_TIMESTAMP() AS load_ts,
    -- NOTE: merged 2 variations for target column 'mu_fund_family_CD'
    CASE
   
    WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) = 'SBB' 
      THEN CASE
   
    WHEN edx.MFIN_SEND_NUMBER IS NOT NULL 
      THEN CASE
   
    WHEN edx.MFIN_ASSET_ORDER = 1 
      THEN '+08830' -- MMK /n 
    WHEN edx.MFIN_ASSET_ORDER = 2 
      THEN '+08831' -- PMMK /n 
    WHEN edx.MFIN_ASSET_ORDER = 3 
      THEN '+08832' -- ING /n 
    WHEN edx.MFIN_ASSET_ORDER = 4 
      THEN '+08833' -- BAL /n 
    WHEN edx.MFIN_ASSET_ORDER = 5 
      THEN '+08834' -- INX /n 
    WHEN edx.MFIN_ASSET_ORDER = 6 
      THEN '+08835' -- CDNEQ /n 
    WHEN edx.MFIN_ASSET_ORDER = 7 
      THEN '+08836' -- USEQ /n 
    WHEN edx.MFIN_ASSET_ORDER = 8 
      THEN '+08837' -- SEC /n 
    WHEN edx.MFIN_ASSET_ORDER = 9 
      THEN '+08838' -- GLO /n 
    WHEN edx.MFIN_ASSET_ORDER = 10 
      THEN '+08839' -- MAPM /n 
    WHEN edx.MFIN_ASSET_ORDER = 11 
      THEN '+08840' -- MAP /n 
    WHEN edx.MFIN_ASSET_ORDER - 12 
      THEN '+08841' -- MAP /n 
    WHEN edx.MFIN_ASSET_ORDER - 13 
      THEN '+08842' -- GIC /n 
    WHEN edx.MFIN_ASSET_ORDER = 14 
      THEN '+12553' -- COMFORT /n 
    WHEN edx.MFIN_ASSET_ORDER = 15 
      THEN '+12554' -- ADVANTAGE /n 
    WHEN edx.MFIN_ASSET_ORDER = 16 
      THEN '+12559' -- CORPORATE /n 
    ELSE '-00004' -- Invalid /n 
  END 
    ELSE -'00001' -- Not Applicable /n 
  END 
    ELSE '-00001' -- Not Applicable /n 
  END AS mu_fund_FAMILY_CD
    -- Source context preserved: FROM ossbr_2_1 mas LEFT JOIN GLSREF ref ON mas.SRSECCODE = ref.sm_SECURITY_CODE LEFT JOIN MFIN ed ON SUBSTRING (ref.SEND_CD, 4, 5) = edx.MFIN_SEND_NUMBER,
    -- NOTE: merged 2 duplicate definitions for target column 'mu_fund_id'
    CASE
   
    WHEN ref.sm_SECURITY_CODE IS NOT NULL 
      THEN ref.SEND_CD 
    ELSE '00000000' 
  END AS mu_fund_ID
    -- Source context preserved: FROM ossbr_2_1 mas LEFT JOIN GLSXREF ref ON mas.SRSECCODE = ref.sm_SECURITY_CODE,
    CAST(NULL AS DATE) AS row_exclsn_dt,
    CASE
   
    WHEN TRY_CAST(mas.SRSECCLAS AS INT) IS NOT NULL 
      THEN mas.SRSECCLAS 
    ELSE '0' 
  END AS SECRTY_CLASS_ID
    -- Source context preserved: FROM ossbr_2_1 mas,
    CAST(1342 AS BIGINT) AS secrty_view_CD,
    TO_DATE(borid, 'YYYY-MM-DD') AS sm_secrty_id,
    CASE
   
    WHEN TRY_CAST(mas.SRSECTYPE AS INT) IS NOT NULL 
      THEN mas.SRSECTYPE 
    ELSE '+00000' 
  END AS sm.SECRTY_TYPE_ID AS sm_secrty_type_id
    -- Source context preserved: FROM ossbr_2_1 mas,
    CAST(239 AS BIGINT) AS source_appl_CD,
    CAST(NULL AS STRING) AS sumary_combin_id,
    CASE
   
    WHEN ref.sm_SECURITY_CODE IS NOT NULL 
      THEN CASE
   
    WHEN mas.SRSECTYPE IN ('380','331') 
      THEN '+01313' 
    ELSE '+01316' 
  END 
    ELSE CASE
   
    WHEN mas.SRSECTYPE = '220' 
      THEN '+01312' 
    WHEN mas.SRSECTYPE IN ('315','600','520','420','310','330','510') 
      THEN '+01314'  
    WHEN mas.SRSECTYPE IN ('551','553','555','559','567','550','556','557','570','581','554','558','568','569','573','562','530') 
      THEN '+01315' 
    WHEN mas.SRSECTYPE IN ('210','230','240') 
      THEN '+01309' 
    WHEN mas.SRSECTYPE IN ('263','265','275','260','261','270','262','276','290','295') 
      THEN '+01310' 
    ELSE '+01316'  
  END 
  END AS tantrum_family_cd LEFT JOIN GLSXREF ref ON mas.SRSECCODE = ref.sm_SECURITY_CODE AS tantrum_family_CD,
    -- NOTE: merged 3 duplicate definitions for target column 'tantrum_issued_in'
    CASE
   
    WHEN ref.sm_SECURITY_CODE IS NOT NULL AND LEFT(ref.SEND_CD, 3) IN ('SBM', 'SBG', 'SBS','SBB', 'SBC') 
      THEN 'Y' 
    WHEN LEFT(mas.SRSHSBESE, 2) = 'SB' 
      THEN 'Y' 
    ELSE 'N' 
  END AS tantrum_issued_in
    -- Source context preserved: FROM ossbr_2_1 mas LEFT JOIN GLSXREF ref ON mas.SRSECCODE = ref.sm_SECURITY_CODE,
    -- NOTE: merged 2 variations for target column 'tantrum_na'
    CASE
   
    WHEN ref.sm_SECURITY_CODE IS NOT NULL 
      THEN RTRIM(ref.FUND_DESC) 
    ELSE RTRIM(mas.SRSHSBESE) 
  END AS tantrum_na
    -- Source context preserved: FROM ossbr_2_1 mas LEFT JOIN GLSXREF ref ON mas.SRSECCODE = ref.sm_SECURITY_CODE,
    -- NOTE: merged 3 duplicate definitions for target column 'tantrumid'
    CASE
  
  
    WHEN LEFT(GLSXREF.SEND_CD, 3) = 'SBB' 
      THEN
    CASE
  
      
    WHEN SUBSTRING(GLSXREF.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM 
      THEN NULL
      
    ELSE
        CASE
  
          
    WHEN SUBSTRING(GLSXREF.SEND_CD, 4, 3) LIKE '[0-9]%' AND SUBSTRING(GLSXREF.SEND_CD, 7, 2) = '*' 
      THEN CONCAT(230000000000, SUBSTRING(GLSXREF.SEND_CD, 4, 3))
          
    WHEN LEN(TRIM(SUBSTRING(GLSXREF.SEND_CD, 4, 5))) > 0 
      THEN CONCAT(STRING_AGG(FORMAT(ASCII(SUBSTRING(GLSXREF.SEND_CD, 4, 1)), '00')), '000')
          
    ELSE CONCAT('23000', STRING_AGG(FORMAT(ASCII(SUBSTRING(GLSXREF.SEND_CD, 4, 1)), '00')))
        
  END
    
  END
  
    ELSE CONCAT('500', STRING_AGG(FORMAT(ASCII(ossbr_2_1.SRSECCODE), '00')))

  END AS tantrum_id
    -- Source context preserved: FROM ossbr_2_1 LEFT JOIN GLSXREF ON ossbr_2_1.SRSECCODE = GLSXREF.sm_SECURITY_CODE LEFT JOIN MFSPRIC ON SUBSTRING(GLSXREF.SEND_CD, 4, 5) = MFSPRIC.PRC_DTL_SEND_NUM,
    CAST('N' AS STRING) AS tct_issue_in,
    CAST('A' AS BIGINT) AS term_family_CD,
    CAST('9999-12-31' AS DATE) AS to_dt,
    CAST(0 AS BIGINT) AS trm_prodct_id,
    CAST(0 AS BIGINT) AS varble_rate_id
FROM step1;
