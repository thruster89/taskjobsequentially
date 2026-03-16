-- Oracle 추출 예시 ({yyyymm}은 자동 치환됨)
SELECT
    CUST_ID,
    CUST_NM,
    AMT,
    YYYYMM
FROM SCHEMA.ORA_TABLE
WHERE YYYYMM = '{yyyymm}'
