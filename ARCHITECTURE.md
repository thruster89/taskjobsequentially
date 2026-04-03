# 코드 설명서 (Architecture)

## 전체 구조

```
sas_to_duckdb.py   ← 범용 실행기 (CLI + 파이프라인 엔진)
dat_loader.py      ← 파일 읽기 유틸리티 (FWF/Pipe/CSV/SAS)
runset.py          ← 런셋 실행기 (여러 JOB 연속 + 예약 실행)
jobs/*.py          ← JOB 정의 파일 (테이블·로직·검증·출력)
runsets/*.py       ← 런셋 설정 파일 (JOB 목록·타임아웃·년월)
```

실행 흐름:

```
CLI (main)
  │
  ├─ load_job_module()     JOB 파일 동적 import
  │
  └─ run_job()             단일 JOB 실행
       ├─ do_load()        1. 파일 → DuckDB 적재
       ├─ job.logic()      2. 비즈니스 로직 (SQL)
       ├─ job.validate()   3. 검증
       └─ do_export()      4. Excel 출력
```

---

## sas_to_duckdb.py 상세

### 모듈 구성

| 영역 | 줄 | 설명 |
|------|-----|------|
| 로거 | 29-63 | `setup_logger()` — 콘솔(INFO) + 파일(DEBUG) 이중 로깅. 한글 폭 고려 정렬 포매터. |
| 설정 | 86-92 | `ROOT`, `MAX_READ_WORKERS`, `LOAD_TIMEOUT`, `ENCODINGS`, `FILE_EXTENSIONS` |
| 유틸 (public) | 99-245 | JOB 파일에서 import하여 사용하는 함수들 |
| 유틸 (private) | 248-383 | 파일 탐색, Oracle DSN, 파일 읽기, upsert |
| JOB 로더 | 389-415 | `load_job_module()` — 동적 import + 필수 속성 검증 |
| 파이프라인 | 422-704 | `do_load()`, `_build_export_query()`, `do_export()`, `run_job()` |
| CLI | 822-892 | `main()` — argparse + JOB 순차 실행 |

### Public 유틸 함수

JOB 파일의 `logic()`과 `validate()`에서 사용합니다.

#### `sql(con, label, query, params=None)`

SQL 실행 후, CREATE TABLE 문이면 생성된 테이블의 건수를 자동 로깅합니다.

```
로그:   결과 테이블                                          45,230건  (0.3초)
```

내부 동작:
1. `con.execute(query, params)` 실행
2. 정규식으로 `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] 테이블명` 패턴 매칭
3. 매칭되면 `SELECT COUNT(*)` 실행하여 건수 로깅

#### `table_exists(con, name)`

`information_schema.tables`에서 테이블 존재 여부를 확인합니다.
JOB 간 의존성 처리(이전 JOB에서 만든 테이블 참조)에 사용합니다.

#### `require_tables(con, *names)`

여러 테이블의 존재 여부를 한번에 확인합니다. 없는 테이블마다 경고 로그를 출력하고 `False`를 반환합니다.
`validate()`에서 `table_exists` 대신 사용하면 테이블 부재 시 조용히 넘어가지 않고 경고가 남습니다.

```
로그:   [WARNING] [--] bs104                                     테이블 없음 — 건너뜀
```

#### `check(con, label, query, expect="zero")`

검증용. SELECT 결과(단일 숫자)를 expect 조건과 비교하여 OK/NG를 로깅합니다.

```
로그:   [OK] 중복 체크                                          0건
       [NG] 데이터 존재 확인                                    0건
```

- `expect="zero"` — 0건이어야 OK (이상 데이터 탐지)
- `expect="nonzero"` — 1건 이상이어야 OK (데이터 존재 확인)
- `expect=100` — 정확히 100건이어야 OK

#### `check_sum(con, label, query)`

합계값 표시용. SELECT 결과(단일 행)의 모든 컬럼을 컬럼명과 함께 로깅합니다.

```
로그:   [OK] 보험료 합계                     RP_PRM:  1,234,567 | AF_PRM:    987,654
```

내부 동작:
1. `con.execute(query).fetchone()` 실행
2. `con.description`에서 컬럼명 추출
3. 숫자는 천단위 콤마, NULL은 "NULL"로 표시

#### `check_diff(con, label, query_a, query_b, group_cols, sum_col, threshold=1)`

두 쿼리 결과를 `group_cols` 기준 FULL OUTER JOIN하여 `sum_col` 차이를 비교합니다.

```
로그:   [NG] 전표 vs 배분                                   차이 3건
              DVCD=110                    A:    1,000,000  B:      999,000  diff:        1,000
              DVCD=210                    A:      500,000  B:      500,500  diff:         -500
              ...
              [합계]                       A:    1,500,000  B:    1,499,500  diff:          500
       → output/diff_전표_vs_배분.csv
```

내부 동작:
1. CTE로 두 쿼리를 `a`, `b`로 정의
2. `FULL OUTER JOIN` + `IS NOT DISTINCT FROM` (NULL-safe 비교)
3. 차이가 `threshold` 초과인 행만 필터
4. 상위 20건 로그 출력 + 합계 표시
5. 차이 있으면 `output/diff_라벨.csv`에 전체 결과 저장

#### `row_count(con, table, group_by=None, where=None)`

테이블 건수를 로깅합니다. 테이블이 없으면 경고 표시.
`group_by` 지정 시 그룹별 건수, `where` 지정 시 조건부 건수를 출력합니다.

```
로그:   [OK] fio841                                         45,230건
       [--] missing_table                              테이블 없음
```

---

### do_load() — 파일 적재 엔진

JOB의 `TABLES` dict를 기반으로 파일을 읽어 DuckDB에 적재합니다.

#### 읽기 전략 (2단계)

```
TABLES
  │
  ├─ native (pipe, csv, fwf+native)  → DuckDB C++ 엔진 직접 읽기 (빠름)
  │   └─ 실패 시                     → pandas 폴백
  │
  └─ other (fwf, sas7bdat, oracle)
      └─ ThreadPoolExecutor  → 병렬 읽기 → 순차 적재
```

FWF 테이블에 `"native": True`를 명시하면 DuckDB SUBSTR 경로로 직접 처리합니다.
한글이 포함된 FWF는 SUBSTR이 글자 단위라 바이트 위치가 밀리므로 native를 쓰면 안 됩니다.

**Native (pipe, csv, fwf+native):**
1. `read_csv()`로 줄 단위 단일 컬럼 `_pipe_raw` 테이블 생성
2. `string_split()` 또는 `SUBSTR()`로 컬럼 분리 → `_pipe_parsed` 임시 테이블
3. `_pipe_parsed` → 실제 테이블로 upsert
4. 실패 시 pandas 폴백 (`read_pipe_dat()`)

**Other (fwf, sas7bdat, oracle):**
1. `ThreadPoolExecutor(workers=4)`로 병렬 읽기 → DataFrame
2. 순차적으로 `_upsert()` — DuckDB는 단일 쓰기만 지원

#### 타임아웃

- `LOAD_TIMEOUT=300`초 (기본)
- Native: `threading.Timer` → `con.interrupt()`로 쿼리 취소
- Other: `fut.result(timeout=N)`

#### 월별 누적 적재 (_upsert)

```
테이블 없음?  → CREATE TABLE AS SELECT *
month_col 있음? → DELETE WHERE month_col LIKE 'YYYYMM%' → INSERT
month_col 없음? → DELETE 전체 → INSERT (전체 교체)
```

---

### _build_export_query() — Export SQL 생성

`EXPORT_SHEETS`의 값(str 또는 dict)을 SQL 문으로 변환합니다.
테이블 키, 시트명, SQL/where 등 모든 문자열에서 `{yyyymm}` 플레이스홀더가 자동 치환됩니다.

```
"시트명"                           → SELECT * FROM tbl
{"sheet": "시트명"}                → SELECT * FROM tbl
{"sql": "SELECT ..."}             → 그대로 사용 (JOIN/UNION 가능)
{"columns": [...]}                → SELECT col1, col2 FROM tbl
{"where": "...", "order_by": ...} → SELECT * FROM tbl WHERE ... ORDER BY ...
```

`"sql"` 키가 있으면 `columns/where/order_by/limit`는 무시됩니다.
`"sql"` 키를 사용할 때 dict key(테이블명)는 실제 테이블과 무관합니다.

---

### do_export() — Excel 출력

1. `_next_output_path()`로 버전 번호 결정 (v0.01 → v0.02 → ...)
2. `out_` 접두사 테이블 자동 추가 (DB에서 `information_schema` 조회)
3. 시트별 SQL 실행 → DataFrame → `to_excel()`
4. 정수/실수 컬럼에 `#,##0` 서식 적용 (E+ 방지)
5. 요약 시트 생성 (`_write_summary_sheet`)
   - 테이블명, 시트명(하이퍼링크), 건수, 소요시간
   - 합계 행 포함

---

### run_job() — 단일 JOB 실행

```python
run_job(con, job_mod, yyyymm, skip_load=False, stages=None,
        only_tables=None, load_timeout=None)
```

- `stages=None` → 전체 실행 (load → logic → validate → export)
- `stages=["logic", "validate"]` → 해당 단계만 실행
- `skip_load=True` → load 생략 (stages=None일 때만 동작)
- `only_tables=["fio841"]` → load 시 해당 테이블만 적재
- 각 단계(logic/validate/export)는 실패해도 다음 단계로 계속 진행

---

### CLI (main)

```
python sas_to_duckdb.py --ym YYYYMM [YYYYMM ...] --job JOB [JOB ...] [옵션]
```

1. argparse로 인자 파싱
2. DuckDB 연결 (`db/ifrs4-expense.duckdb`)
3. JOB 모듈 순차 로드 (`load_job_module`)
4. **월별 → JOB 순차 실행** (`--ym`이 복수이면 월 단위로 반복)
   - 예: `--ym 202601 202602 --job job1 job2` → `202601/job1 → 202601/job2 → 202602/job1 → 202602/job2`
5. 연결 종료

---

## dat_loader.py 상세

파일 읽기 전용 유틸리티. `sas_to_duckdb.py`에서 import하여 사용하며, 단독 CLI로도 실행 가능합니다.

### 파일 열기

| 함수 | 용도 |
|------|------|
| `open_file(path, encoding)` | 텍스트 모드. zip/gz/일반 자동 감지 |
| `open_file_binary(path)` | 바이너리 모드. FWF 바이트 슬라이싱용 |

### 읽기 함수 (pandas 경유)

| 함수 | 파일 유형 | 핵심 로직 |
|------|-----------|-----------|
| `read_fwf_dat()` | 고정폭 (FWF) | 바이트 슬라이싱 → 컬럼별 decode. cp949 한글 위치 안 밀림 |
| `read_pipe_dat()` | 파이프 구분자 | `pd.read_csv(sep="\|")` + 인코딩 fallback |
| `read_csv_file()` | CSV | `pd.read_csv()` + 인코딩 fallback |
| `read_sas7bdat_file()` | SAS 데이터셋 | `pyreadstat.read_sas7bdat()`. 컬럼 메타데이터 자동 추출 |

### 읽기 함수 (DuckDB 네이티브)

pandas를 우회하여 DuckDB C++ 엔진으로 직접 읽습니다. 대용량 파일에서 빠릅니다.

| 함수 | 파일 유형 | 핵심 로직 |
|------|-----------|-----------|
| `read_fwf_duckdb()` | 고정폭 | `read_csv(delim='\x01')` → 줄 단위 읽기 → `SUBSTR()` + TRIM 분리 |
| `read_pipe_duckdb()` | 파이프 | `read_csv(all_varchar=true)` → 멀티스레드 파싱 → SELECT + TRY_CAST |
| `read_csv_duckdb()` | CSV | `read_csv()` 직접 사용 |

**read_pipe_duckdb 처리 흐름:**
1. 인코딩 감지 (`charset_normalizer`) → non-utf8이면 `decompress_gz`로 cp949→utf-8 변환
2. `read_csv(all_varchar=true)` — DuckDB 멀티스레드 CSV 파싱 (TRIM 없음)
3. `SELECT column000 AS name, TRY_CAST(column001 AS DOUBLE) AS amt ...` — 컬럼 리네임 + 캐스팅
4. `select_cols` 지정 시 해당 컬럼만 materialize (불필요 컬럼 제외)
5. `numeric` → DOUBLE, `bigint` → BIGINT 캐스팅
6. 인코딩 에러 시 즉시 `decompress_gz` 폴백 (다른 인코딩 재시도 없음)

**인코딩 처리:**
- `.gz` + non-utf8: `decompress_gz`로 해제 + cp949→utf-8 변환 (DuckDB `.gz + cp949` 크래시 방지)
- 변환 결과는 `.utf8.dat`로 캐시 (원본보다 새로우면 스킵)
- `preconvert: True` 설정 시 인코딩 감지 없이 바로 변환

### 공통 유틸

| 함수 | 설명 |
|------|------|
| `try_read()` | 인코딩 fallback 래퍼. encodings 순서대로 시도하여 성공한 결과 반환 |
| `_strip_str()` | object 컬럼 전체 공백 제거 (숫자형 대상 제외) |
| `_cast_numeric()` | 지정 컬럼을 `pd.to_numeric(errors="coerce")` |
| `preview()` | DataFrame 미리보기 (shape + 컬럼명 + head) |

---

## JOB 파일 구조

각 JOB은 독립된 Python 모듈입니다. `sas_to_duckdb.py`가 동적으로 import합니다.

### 필수 정의

```python
NAME = "job1"                    # JOB 이름 (파일명·로그에 사용)
TABLES = { ... }                 # 로드할 테이블 정의
def logic(con, yyyymm): ...      # 비즈니스 로직 (DuckDB SQL 실행)
def validate(con, yyyymm): ...   # 검증 (건수·합계·차이 체크)
```

### 선택 정의

```python
DESC = "설명"                    # JOB 설명
EXPORT_SHEETS = { ... }          # Excel 출력 시트 매핑
```

### TABLES dict 구조

```python
TABLES = {
    "테이블명": {
        "type": "fwf|pipe|csv|sas7bdat|oracle",
        "file": "파일명_{yyyymm}.DAT",        # {yyyymm} 자동 치환
        "month_col": "CLS_YYMM",              # 월별 누적 기준 컬럼 (None=전체 교체)
        "numeric": ["AMT", "CNT"],             # → DOUBLE 캐스팅
        "bigint": ["SEQ", "ID"],               # → BIGINT 캐스팅 (pipe 전용)
        "cols": [...],                          # fwf: [(start,end),"name"] / pipe: ["name"]
        "select_cols": ["AMT", "SEQ"],         # DB에 적재할 컬럼만 (pipe 전용, 미지정=전체)
        "preconvert": True,                    # 인코딩 사전 변환 강제 (pipe 전용)
        # Oracle 전용:
        "sql": "SELECT ...",
        "dsn": "host:port/service",
        "user": "...", "password": "...",
    }
}
```

### JOB 간 데이터 공유

모든 JOB이 동일한 DuckDB 파일(`ifrs4-expense.duckdb`)을 사용합니다.
앞선 JOB에서 만든 테이블을 뒤 JOB에서 참조할 수 있습니다.

```python
# job2.py의 logic()에서 job1이 만든 테이블 참조
def logic(con, yyyymm):
    if table_exists(con, "BASE_DATA_CH"):
        sql(con, "결합", "CREATE OR REPLACE TABLE merged AS SELECT ... FROM BASE_DATA_CH ...")
```

---

## runset.py — 런셋 실행기

여러 JOB을 연속 실행하는 래퍼. subprocess로 `sas_to_duckdb.py`를 호출하여 JOB 간 격리.

```
runset.py --config runsets/daily.py [--at 시각] [--timeout 초]
  │
  ├─ load_runset()         설정 파일 로드 (YM, JOBS, TIMEOUT)
  ├─ wait_until()          예약 시각까지 대기 (선택)
  └─ for job in JOBS:
       └─ subprocess.run() → sas_to_duckdb.py --ym ... --job ...
```

- 전체 타임아웃: 초과 시 남은 JOB 건너뜀
- JOB별 타임아웃: `subprocess.TimeoutExpired`로 개별 종료
- 예약 실행: `--at 06:00`, `--at "2026-04-04 06:00"`, `--at +30m`, `--at +2h`

---

## 데이터 흐름 요약

```
data/YYYYMM/*.DAT          ──┐
data/YYYYMM/*.sas7bdat      ──┤── do_load() ──→ DuckDB 테이블
Oracle DB                   ──┘
                                      │
                                job.logic()    SQL로 가공
                                      │
                                job.validate() 건수·합계·차이 검증
                                      │
                                do_export()    Excel 출력
                                      │
                              output/job_YYYYMM_vN.NN.xlsx
```
