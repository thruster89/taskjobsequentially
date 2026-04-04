"""
JOB 4: SQL 파라미터 활용 예시

실행:
  python sas_to_duckdb.py --ym 202603 --job jobs/job4.py
"""
from sas_to_duckdb import sql, sql_file, table_exists, require_tables, check, check_sum, row_count

NAME = "job4"
DESC = "SQL 파라미터 활용 예시"

# 커스텀 파라미터 (기본 SDM/LM/LM2/YYYY/MM 외 추가)
# def PARAMS(yyyymm):
#     return {"FY_START": yyyymm[:4] + "01"}

# ══════════════════════════════════════════════
# 테이블 정의
# ══════════════════════════════════════════════
TABLES = {
    # 테이블명에 {yyyymm} 사용 → dambo202603
    "dambo{yyyymm}": {
        "type": "pipe",
        "file": "btLtrJ930_020_{yyyymm}??_*.dat.gz",
        "desc": "930 담보별",
        "delimiter": "|",
        "month_col": None,
        "numeric": ["PRM", "AMT"],
        "bigint": ["SEQ_NO"],
        "select_cols": ["CLS_YYMM", "PLYNO", "CVRCD", "PRM", "AMT", "SEQ_NO"],
        # "preconvert": True,  # 인코딩 에러 시
    },
}

EXPORT_SHEETS = {
    # 기본: SELECT * FROM 테이블
    "dambo{yyyymm}": "담보별원본",

    # SQL에서 ${SDM}, ${LM} 사용
    "monthly_compare": {
        "sheet": "당월전월비교",
        "sql": """
            SELECT a.CVRCD,
                   a.AMT AS 당월_AMT,
                   b.AMT AS 전월_AMT,
                   a.AMT - COALESCE(b.AMT, 0) AS 증감
            FROM dambo${SDM} a
            LEFT JOIN dambo${LM} b ON a.CVRCD = b.CVRCD
        """,
    },

    # WHERE 절에 ${SDM} 사용
    "filtered": {
        "sheet": "당월필터",
        "columns": ["PLYNO", "CVRCD", "PRM"],
        "where": "CLS_YYMM = '${SDM}'",
        "order_by": "PRM DESC",
        "limit": 1000,
    },
}


# ══════════════════════════════════════════════
# 비즈니스 로직
# ══════════════════════════════════════════════
def logic(con, yyyymm):

    # 인라인 SQL에서 ${SDM}, ${LM} 사용
    sql(con, "당월 집계", """
        CREATE OR REPLACE TABLE agg_${SDM} AS
        SELECT CVRCD,
               COUNT(*) AS CNT,
               SUM(PRM) AS TOTAL_PRM
        FROM dambo${SDM}
        WHERE CLS_YYMM = '${SDM}'
        GROUP BY CVRCD
    """)

    # 전월 테이블이 있으면 비교
    if table_exists(con, f"dambo{yyyymm}") and table_exists(con, f"agg_{yyyymm}"):
        sql(con, "당월 vs 전월 증감", """
            CREATE OR REPLACE TABLE compare_${SDM} AS
            SELECT COALESCE(a.CVRCD, b.CVRCD) AS CVRCD,
                   COALESCE(a.TOTAL_PRM, 0) AS 당월,
                   COALESCE(b.TOTAL_PRM, 0) AS 전월,
                   COALESCE(a.TOTAL_PRM, 0) - COALESCE(b.TOTAL_PRM, 0) AS 증감
            FROM agg_${SDM} a
            FULL OUTER JOIN agg_${LM} b ON a.CVRCD = b.CVRCD
        """)

    # SQL 파일도 가능 (파일 안에서 ${SDM}, ${LM} 사용)
    # sql_file(con, "외부 SQL", "sql/job4_query.sql")


# ══════════════════════════════════════════════
# 검증
# ══════════════════════════════════════════════
def validate(con, yyyymm):
    row_count(con, f"dambo{yyyymm}")
    row_count(con, f"agg_{yyyymm}")

    if table_exists(con, f"compare_{yyyymm}"):
        row_count(con, f"compare_{yyyymm}")
        check_sum(con, "당월 보험료 합계",
                  f"SELECT SUM(당월) AS 당월, SUM(전월) AS 전월, SUM(증감) AS 증감 FROM compare_{yyyymm}")
