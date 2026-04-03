r"""
read_csv 병목 구간 측정 스크립트

사용법:
  python bench_read_csv.py <utf8_dat_file> [delimiter] [select_count]

예시:
  python bench_read_csv.py D:\python-tasks\sas-sunset2\data\202603\btLtrJ930_020_20260331_20260331.utf8.dat "|" 92
"""
import sys
import time
import duckdb

def main():
    path = sys.argv[1]
    delim = sys.argv[2] if len(sys.argv) > 2 else "|"
    select_n = int(sys.argv[3]) if len(sys.argv) > 3 else None

    con = duckdb.connect()
    con.execute("SET memory_limit = '47GB'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET temp_directory = 'duckdb_tmp'")

    csv_src = f"read_csv('{path}', delim='{delim}', header=false, encoding='utf-8', all_varchar=true)"

    # 0단계: 컬럼 수 확인
    desc = con.execute(f"SELECT * FROM {csv_src} LIMIT 1").description
    total_cols = len(desc)
    col_names = [d[0] for d in desc]
    print(f"컬럼 수: {total_cols} ({col_names[0]} ~ {col_names[-1]})")

    # 1단계: 순수 파싱 (COUNT만)
    print("\n[1단계] SELECT COUNT(*) — 순수 파싱")
    t0 = time.time()
    cnt = con.execute(f"SELECT COUNT(*) FROM {csv_src}").fetchone()[0]
    t1 = time.time()
    print(f"  {cnt:,}행, {t1-t0:.1f}초")

    # 2단계: SELECT * → 테이블 저장 (전체 컬럼, 캐스팅 없음)
    print("\n[2단계] CREATE TABLE AS SELECT * — 전체 컬럼 VARCHAR 저장")
    con.execute("DROP TABLE IF EXISTS bench_all")
    t0 = time.time()
    con.execute(f"CREATE TABLE bench_all AS SELECT * FROM {csv_src}")
    t2 = time.time()
    print(f"  {t2-t0:.1f}초")

    # 3단계: SELECT N개 컬럼만 → 테이블 저장
    if select_n and select_n < total_cols:
        cols = ", ".join(col_names[:select_n])
        print(f"\n[3단계] CREATE TABLE AS SELECT {select_n}개 컬럼 — 부분 컬럼 저장")
        con.execute("DROP TABLE IF EXISTS bench_partial")
        t0 = time.time()
        con.execute(f"CREATE TABLE bench_partial AS SELECT {cols} FROM {csv_src}")
        t3 = time.time()
        print(f"  {t3-t0:.1f}초")

    # 4단계: TRY_CAST 포함
    if select_n and select_n < total_cols:
        cast_exprs = []
        for i in range(select_n):
            if i < 5:  # 처음 5개만 DOUBLE로 캐스팅 테스트
                cast_exprs.append(f"TRY_CAST({col_names[i]} AS DOUBLE) AS {col_names[i]}")
            else:
                cast_exprs.append(col_names[i])
        cast_clause = ", ".join(cast_exprs)
        print(f"\n[4단계] CREATE TABLE AS SELECT + TRY_CAST(5개) — 캐스팅 포함")
        con.execute("DROP TABLE IF EXISTS bench_cast")
        t0 = time.time()
        con.execute(f"CREATE TABLE bench_cast AS SELECT {cast_clause} FROM {csv_src}")
        t4 = time.time()
        print(f"  {t4-t0:.1f}초")

    # 정리
    con.execute("DROP TABLE IF EXISTS bench_all")
    con.execute("DROP TABLE IF EXISTS bench_partial")
    con.execute("DROP TABLE IF EXISTS bench_cast")
    con.close()

    print("\n완료")

if __name__ == "__main__":
    main()
