# SAS → DuckDB 사업비배분 파이프라인 사용자 가이드

## 개요

SAS에서 수행하던 사업비배분 JOB을 Python/DuckDB로 전환한 파이프라인입니다.

- **JOB 파일**(`jobs/*.py`)에 테이블 정의·로직·검증을 독립적으로 작성
- **범용 실행기**(`sas_to_duckdb.py`)가 `load → logic → validate → export` 순서로 실행
- **하나의 DB**(`ifrs4-expense.duckdb`)에 월별 데이터가 누적

```
 1차 파일 수신              2차 파일 수신              3차 파일 수신
      ↓                          ↓                          ↓
  jobs/job1.py               jobs/job2.py               jobs/job3.py
  ┌──────────────┐           ┌──────────────┐           ┌──────────────┐
  │ load  fio841 │           │ load  sa01   │           │ load  erp    │
  │       fio843 │           │       sa02   │           │       bs101  │
  │ logic 집계   │           │ logic SA변환 │           │       bs104  │
  │ validate     │           │ validate     │           │       bs105  │
  │ export xlsx  │           │ export xlsx  │           │ validate     │
  └──────────────┘           └──────────────┘           │ export xlsx  │
                                                        └──────────────┘
```

---

## 요구사항

```bash
pip install duckdb pandas openpyxl
# sas7bdat 파일을 읽는 경우:
pip install pyreadstat
# Oracle DB에서 추출하는 경우:
pip install oracledb
```

---

## 폴더 구조

```
프로젝트/
├── sas_to_duckdb.py       ← 범용 실행기 (모든 JOB 공통)
├── dat_loader.py          ← DAT 파일 읽기 유틸리티
├── jobs/
│   ├── job1.py            ← 1차: 보험료 집계
│   ├── job2.py            ← 2차: 배분 결과 (파일 수신 후 작성)
│   └── job3.py            ← 3차: 마감 검증 (파일 수신 후 작성)
├── data/
│   ├── 202601/            ← --ym 202601 일 때 탐색 위치
│   │   ├── fioBtLtrJ841_01_202601.DAT  (또는 .zip / .dat.gz)
│   │   ├── RS100_202601.DAT
│   │   └── ...
│   └── 202602/
├── db/
│   └── ifrs4-expense.duckdb    ← 월별 데이터 누적
├── output/
│   ├── job1_202601.xlsx
│   ├── job2_202601.xlsx
│   └── job3_202601.xlsx
└── logs/
```

파일 확장자는 자동 탐색: `.zip` → `.dat.gz` → `.DAT` → `.dat` → `.prn` → `.csv` → `.csv.gz` → `.sas7bdat`

---

## 실행 방법

### 기본: JOB 파일 지정 실행

```bash
# 1차 파일 수신 후
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py

# 2차 파일 수신 후
python sas_to_duckdb.py --ym 202601 --job jobs/job2.py

# 3차 파일 수신 후
python sas_to_duckdb.py --ym 202601 --job jobs/job3.py
```

### 여러 JOB 한번에 실행

```bash
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py jobs/job2.py jobs/job3.py
```

순서대로 실행됩니다.

### 여러 월 연속 실행

```bash
python sas_to_duckdb.py --ym 202601 202602 --job jobs/job1.py jobs/job2.py
```

월별 → JOB 순서로 순차 실행됩니다:
`202601/job1 → 202601/job2 → 202602/job1 → 202602/job2`

### 로드 생략 (이미 적재된 데이터로 로직만 재실행)

```bash
python sas_to_duckdb.py --ym 202601 --job jobs/job2.py --skip-load
```

### DEBUG 로그

```bash
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py -v
```

---

## CLI 옵션

| 옵션 | 단축 | 설명 | 기본값 |
|------|------|------|--------|
| `--ym YYYYMM [...]` | | 처리할 년월 (복수 가능) | `202601` |
| `--job FILE [...]` | `-j` | JOB 파일 경로 (필수) | |
| `--skip-load` | | LOAD 단계 생략 | OFF |
| `--verbose` | `-v` | DEBUG 로그 콘솔 출력 | OFF |
| `--stage STAGE [...]` | `-s` | 특정 단계만 실행 (load/logic/validate/export) | 전체 |
| `--tables TBL [...]` | `-t` | 로드할 테이블만 지정 | 전체 |
| `--load-timeout SEC` | | 테이블당 최대 읽기 시간(초), 0=무제한 | 300 |

### 단계별 실행 예시

```bash
# load만 실행
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py --stage load

# logic + validate만 재실행 (이미 로드된 데이터 사용)
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py -s logic validate

# 특정 테이블만 로드
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py -s load -t fio841

# export만 재실행
python sas_to_duckdb.py --ym 202601 --job jobs/job2.py -s export
```

---

## 월별 누적

하나의 DB(`ifrs4-expense.duckdb`)에 월별 데이터가 쌓입니다.

```bash
# 1월 적재
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py

# 2월 적재 → 1월 데이터 유지, 2월 추가
python sas_to_duckdb.py --ym 202602 --job jobs/job1.py

# 1월 재적재 → 1월만 교체, 2월 유지
python sas_to_duckdb.py --ym 202601 --job jobs/job1.py
```

각 테이블의 `month_col`(예: `CLS_YYMM`, `SLPDT`) 기준으로 해당 월 DELETE 후 INSERT.

---

## JOB 파일 구조

각 JOB은 `jobs/` 폴더의 독립된 Python 파일입니다.

```python
# jobs/job_example.py
"""설명"""
from sas_to_duckdb import sql, table_exists

NAME = "job_example"                # JOB 이름 (필수)
DESC = "예시 JOB"                   # 설명 (선택)

TABLES = {                          # 테이블 정의 (필수)
    # ── 유형A: 고정폭(fwf) ──
    "fio841": {
        "type": "fwf",
        "native": True,             # 한글 없으면 DuckDB 네이티브 (빠름)
        "file": "fioBtLtrJ841_01_{yyyymm}.DAT",
        "desc": "수입보험료",
        "month_col": "CLS_YYMM",
        "numeric": ["RP_PRM", "AF_PRM"],
        "cols": [                   # (시작, 끝) 바이트 위치 + 컬럼명
            ((0,   6),  "CLS_YYMM"),
            ((16, 32),  "PLYNO"),
            ((32, 48),  "RP_PRM"),
        ],
    },
    # ── 유형B: 파이프 구분자(pipe) ──
    "sa01": {
        "type": "pipe",
        "file": "RS100_{yyyymm}.DAT",
        "desc": "유지비 배분결과",
        "month_col": "SLPDT",
        "numeric": ["DV_RT", "DVAMT"],
        "cols": ["SLPDT", "SLPNO", "SLP_LNNO", "DVAMT"],
    },
    # ── 유형C: SAS 데이터셋(sas7bdat) ──
    "ey_table": {
        "type": "sas7bdat",
        "file": "EY_A{yyyymm}.sas7bdat",
        "desc": "이연 데이터",
        "month_col": None,          # 없으면 전체 교체
        "numeric": ["AMT"],         # 대부분 자동 감지, 추가 캐스팅 필요 시 지정
        # cols 불필요 — sas7bdat는 파일 내 컬럼 메타데이터 사용
    },
    # ── 유형D: Oracle DB ──
    # 접속정보는 oracle_config.py에서 관리
    "ora_table": {
        "type": "oracle",
        **ORA_DEV,                  # from oracle_config import ORA_DEV
        "sql": "SELECT * FROM schema.table WHERE yyyymm = '{yyyymm}'",
        "desc": "Oracle에서 추출",
        "month_col": None,          # 전체 교체
    },
}

EXPORT_SHEETS = {                   # 시트맵 (선택)
    # 기본: 테이블 → 시트명
    "my_table": "시트명",

    # SQL 직접 지정
    "raw_table": {
        "sheet": "필터된시트",
        "sql": "SELECT a, b, a+b AS total FROM raw_table WHERE amt > 0",
    },

    # 컬럼 선택 + 조건 + 정렬 + 건수제한
    "big_table": {
        "sheet": "요약",
        "columns": ["col1", "col2", "col3"],
        "where": "region = '서울'",
        "order_by": "col1 DESC",
        "limit": 1000,
    },
}

def logic(con, yyyymm):             # 비즈니스 로직 (필수)
    sql(con, "결과 테이블", """
        CREATE OR REPLACE TABLE result AS
        SELECT ... FROM my_table
    """)

def validate(con, yyyymm):          # 검증 (필수)
    if table_exists(con, "result"):
        cnt = con.execute("SELECT COUNT(*) FROM result").fetchone()[0]
        print(f"  result: {cnt:,}건")
```

### 필수 항목

| 항목 | 타입 | 설명 |
|------|------|------|
| `NAME` | `str` | JOB 이름 (출력·Excel 파일명에 사용) |
| `TABLES` | `dict` | 로드할 테이블 정의 |
| `logic(con, yyyymm)` | 함수 | 비즈니스 로직 (SQL 실행) |
| `validate(con, yyyymm)` | 함수 | 검증 로직 |

### 선택 항목

| 항목 | 타입 | 설명 |
|------|------|------|
| `DESC` | `str` | JOB 설명 |
| `EXPORT_SHEETS` | `dict` | `{테이블명: 시트명 or dict}` — 없으면 export 생략 |

### EXPORT_SHEETS 상세

값을 **문자열**로 주면 기존처럼 `SELECT *` 전체 추출, **dict**로 주면 세밀한 제어가 가능합니다.

```python
EXPORT_SHEETS = {
    # ① 기본 — SELECT * FROM tbl
    "my_table": "시트명",

    # ② SQL 직접 지정 (가장 유연)
    "raw_table": {
        "sheet": "필터된시트",
        "sql": "SELECT a, b, a+b AS total FROM raw_table WHERE amt > 0",
    },

    # ③ 컬럼 선택만
    "big_table": {
        "sheet": "일부컬럼",
        "columns": ["col1", "col2", "col3"],
    },

    # ④ 조건 + 정렬 + 건수제한
    "summary": {
        "sheet": "서울TOP1000",
        "columns": ["region", "amt"],
        "where": "region = '서울'",
        "order_by": "amt DESC",
        "limit": 1000,
    },

    # ⑤ 두 테이블 JOIN (sql 키 사용 시 dict key는 자유)
    "join_result": {
        "sheet": "보험료_조인",
        "sql": """
            SELECT a.*, b.CUST_NM
            FROM tfc81 a JOIN fio841 b ON a.POL_NO = b.POL_NO
        """,
    },

    # ⑥ UNION ALL
    "union_result": {
        "sheet": "전체합산",
        "sql": "SELECT * FROM tfc81 UNION ALL SELECT * FROM tfc82",
    },

    # ⑦ {yyyymm} 플레이스홀더 (테이블 키·시트명·SQL에서 자동 치환)
    "tfc81_{yyyymm}": "tfc81_{yyyymm}",
    "monthly_data": {
        "sheet": "월별_{yyyymm}",
        "where": "YYYYMM = '{yyyymm}'",
    },
}
```

| dict 키 | 필수 | 설명 |
|----------|------|------|
| `sheet` | X | 시트명 (생략 시 테이블명 사용) |
| `sql` | X | SQL 직접 지정 — 이 키가 있으면 아래 옵션 무시 |
| `columns` | X | 추출할 컬럼 리스트 (생략 시 `*`) |
| `where` | X | WHERE 조건 |
| `order_by` | X | ORDER BY 절 |
| `limit` | X | 최대 건수 |

> `sql`을 지정하면 `columns`, `where`, `order_by`, `limit`는 무시됩니다.
> `sql` 키를 사용할 때 dict key는 실제 테이블명이 아니어도 됩니다 (JOIN, UNION 등에 활용).
> `out_` 접두사 테이블은 여전히 자동 추가됩니다.
> 테이블 키, 시트명, SQL/where 등 모든 문자열에서 `{yyyymm}` 플레이스홀더가 자동 치환됩니다 (f-string 불필요).

### TABLES 타입별 설정

| 타입 | `type` | 필수 키 | 설명 |
|------|--------|---------|------|
| 고정폭 | `fwf` | `cols` (바이트 위치) | DAT 파일 고정폭 파싱 |
| 파이프 구분자 | `pipe` | `cols` (컬럼명 리스트) | `\|` 구분 DAT 파일 |
| SAS 데이터셋 | `sas7bdat` | — | pyreadstat로 읽기 |
| Oracle DB | `oracle` | `sql`, `dsn` | Oracle에서 SQL로 추출 |

#### FWF native 옵션

한글이 없는 FWF 파일은 `"native": True`를 추가하면 DuckDB C++ 엔진으로 직접 처리하여 속도가 크게 향상됩니다.

```python
"fio841": {
    "type": "fwf",
    "native": True,          # ← 한글 없는 파일만! DuckDB SUBSTR로 직접 파싱
    "file": "fioBtLtrJ841_01_{yyyymm}.DAT",
    "cols": [((0, 6), "CLS_YYMM"), ...],
}
```

> **주의:** 한글(cp949 멀티바이트)이 포함된 FWF는 `native` 사용 금지.
> DuckDB `SUBSTR()`은 글자 단위이므로 바이트 위치가 밀립니다.

#### Oracle DSN 형식

```python
# SERVICE_NAME 방식 (슬래시)
"dsn": "host:1523/SERVICE_NAME"

# SID 방식 (콜론 3개)
"dsn": "host:1523:SID_NAME"

# 또는 명시적으로 sid / service_name 키 사용
"dsn": "host:1523",
"sid": "SID_NAME",             # SID 방식
# "service_name": "SVC_NAME",  # SERVICE_NAME 방식
```

### JOB 파일에서 사용 가능한 유틸

```python
from sas_to_duckdb import sql, sql_file, table_exists, require_tables, check, check_sum, check_diff, row_count
```

| 함수 | 용도 | 사용 단계 |
|------|------|-----------|
| `sql(con, label, query)` | SQL 실행 + CREATE TABLE이면 건수 로깅 | logic |
| `sql_file(con, label, path, **params)` | SQL 파일 읽어서 실행 (`{yyyymm}` 등 치환) | logic |
| `table_exists(con, name)` | 테이블 존재 여부 확인 | logic / validate |
| `require_tables(con, *names)` | 여러 테이블 존재 확인 + 없으면 경고 로그 | validate |
| `row_count(con, table, group_by, where)` | 테이블 건수 로깅 (조건부/그룹별) | validate |
| `check(con, label, query, expect)` | 건수 검증 (0건/N건/1건 이상) | validate |
| `check_sum(con, label, query)` | 합계값 표시 (여러 컬럼 지원) | validate |
| `check_diff(con, label, qA, qB, group_cols, sum_col)` | 두 쿼리 결과 차이 비교 | validate |

#### 사용 예시

```python
def logic(con, yyyymm):
    sql(con, "결과 테이블", """
        CREATE OR REPLACE TABLE result AS
        SELECT ... FROM my_table
    """)

    # SQL 파일에서 읽기 (긴 쿼리 분리 시 유용)
    sql_file(con, "SA 변환", "sql/job2_sa_convert.sql", yyyymm=yyyymm)

def validate(con, yyyymm):
    # 건수 확인
    row_count(con, "result")

    # 조건부 건수 확인
    row_count(con, "result", where=f"YYYYMM = '{yyyymm}'")

    # 그룹별 + 조건부 건수
    row_count(con, "result", group_by="TYPE_CD", where=f"YYYYMM = '{yyyymm}'")

    # 이상 데이터 0건이어야 정상
    check(con, "중복 체크", "SELECT COUNT(*) FROM result GROUP BY key HAVING COUNT(*) > 1")

    # 1건 이상 존재해야 정상
    check(con, "데이터 존재", "SELECT COUNT(*) FROM result", expect="nonzero")

    # 합계값 표시
    check_sum(con, "AMT 합계", "SELECT SUM(AMT) AS AMT FROM result")
    check_sum(con, "보험료 합계",
              "SELECT SUM(RP_PRM) AS RP_PRM, SUM(AF_PRM) AS AF_PRM FROM fio841")

    # 두 테이블 차이 비교 (group_cols 기준 FULL OUTER JOIN)
    if require_tables(con, "slp", "sa"):  # 테이블 없으면 경고 로그 후 건너뜀
        check_diff(con, "전표 vs 배분",
                   "SELECT DVCD, SUM(AMT) AS AMT FROM slp GROUP BY DVCD",
                   "SELECT DVCD, SUM(AMT) AS AMT FROM sa  GROUP BY DVCD",
                   group_cols=["DVCD"], sum_col="AMT")
```

#### check 함수 expect 옵션

| expect | 의미 | OK 조건 |
|--------|------|---------|
| `"zero"` (기본) | 이상 데이터 없어야 함 | 0건 |
| `"nonzero"` | 데이터 존재해야 함 | 1건 이상 |
| `int` (예: `100`) | 정확히 N건이어야 함 | N건 |

#### check_diff 동작

- 두 쿼리를 `group_cols` 기준 FULL OUTER JOIN
- `sum_col` 차이가 `threshold`(기본 1) 초과인 행을 출력
- 차이 있으면 `output/diff_라벨.csv`에 자동 저장

---

## 현재 JOB 구성

### jobs/job1.py — 원천 데이터 집계 (보험료)

| 단계 | 내용 |
|------|------|
| LOAD | `fio841` (수입보험료), `fio843` (사업비배분 보유건수) |
| LOGIC | `BASE_DATA_CH`, `BASE_DATA_CH_843`, `BASE_DATA_CH2`, `CH` |
| VALIDATE | 각 테이블 건수 확인 |
| EXPORT | `job1_YYYYMM.xlsx` |

### jobs/job2.py — 배분 결과 (SA 변환·집계)

| 단계 | 내용 |
|------|------|
| LOAD | `sa01` (유지비·신계약비), `sa02` (손해조사비) |
| LOGIC | `sa11` → `sa12` → `sa20` → `sa_{yyyymm}`, `TEMP01` (이연), `out_SA000`, `out_SA001` |
| VALIDATE | 건수, 전표 vs 배분결과 시산표 비교 |
| EXPORT | `job2_YYYYMM.xlsx` |

### jobs/job3.py — 마감 검증 (ERP·BS)

| 단계 | 내용 |
|------|------|
| LOAD | `erp`, `bs101`, `bs104`, `bs105` |
| LOGIC | 확장 가능 (현재 pass) |
| VALIDATE | IBNR 담보코드 누락, 자동차 TM/CM, 재마감 금액 확인 |
| EXPORT | `job3_YYYYMM.xlsx` |

---

## 새 JOB 추가

1. `jobs/` 폴더에 새 파일 생성 (예: `jobs/job4.py`)
2. `NAME`, `TABLES`, `logic()`, `validate()` 정의
3. 실행: `python sas_to_duckdb.py --ym 202601 --job jobs/job4.py`

기존 JOB 파일은 수정할 필요 없습니다.

---

## 출력 파일

JOB별 Excel 파일이 `output/` 폴더에 생성됩니다.

```
output/
├── job1_202601.xlsx
├── job2_202601.xlsx
└── job3_202601.xlsx
```

- 첫 번째 시트: **요약** (테이블명·시트명·건수·소요시간, 시트 하이퍼링크 포함)
- `out_` 접두사 테이블은 자동으로 시트에 포함
- 시트명은 31자에서 자동 잘림 (openpyxl 제한)
- 정수/실수 컬럼에 `#,##0` 서식 자동 적용 (E+ 지수 표기 방지)

### 버전 관리

실행할 때마다 마이너 버전이 자동 증가합니다.

```
job1_202601_v0.01.xlsx   ← 첫 실행
job1_202601_v0.02.xlsx   ← 재실행
job1_202601_v0.03.xlsx   ← 재실행
job1_202601_v1.00.xlsx   ← 유저가 직접 v1.0으로 올림
job1_202601_v1.01.xlsx   ← 이후 재실행
```

메이저 번호는 유저가 파일명을 수동으로 변경하여 올립니다.

---

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| `파일 없음: xxx.*` | data/YYYYMM/ 폴더에 해당 파일 없음 | 파일명·경로 확인, 압축 형식 확인 |
| `JOB 파일에 'NAME' 없음` | JOB 파일에 필수 항목 누락 | NAME, TABLES, logic, validate 확인 |
| `ey_a2601 없음` | 이연 계산용 테이블 미등록 | TABLES에 ey_a2601 추가하거나 별도 JOB에서 로드 |
| 인코딩 에러 | cp949/utf-8 모두 실패 | dat_loader.py의 ENCODINGS에 인코딩 추가 |
| 이전 JOB 결과 참조 실패 | 앞선 JOB을 실행하지 않음 | 앞 JOB 먼저 실행 (DB에 데이터 있어야 함) |
| `oracledb 패키지가 필요합니다` | oracledb 미설치 | `pip install oracledb` |
| Oracle 연결 실패 | DSN/계정 오류 | `dsn`, `user`, `password` 확인. thin 모드로 Oracle Client 불필요 |
