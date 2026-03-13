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
# sas7bdat 파일을 읽는 경우 추가:
pip install pyreadstat
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
| `--ym YYYYMM` | | 처리할 년월 | `202601` |
| `--job FILE [...]` | `-j` | JOB 파일 경로 (필수) | |
| `--skip-load` | | LOAD 단계 생략 | OFF |
| `--verbose` | `-v` | DEBUG 로그 콘솔 출력 | OFF |

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
        "file": "fioBtLtrJ841_01_{YYYYMM}.DAT",
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
        "file": "RS100_{YYYYMM}.DAT",
        "desc": "유지비 배분결과",
        "month_col": "SLPDT",
        "numeric": ["DV_RT", "DVAMT"],
        "cols": ["SLPDT", "SLPNO", "SLP_LNNO", "DVAMT"],
    },
    # ── 유형C: SAS 데이터셋(sas7bdat) ──
    "ey_table": {
        "type": "sas7bdat",
        "file": "EY_A{YYYYMM}.sas7bdat",
        "desc": "이연 데이터",
        "month_col": None,          # 없으면 전체 교체
        "numeric": ["AMT"],         # 대부분 자동 감지, 추가 캐스팅 필요 시 지정
        # cols 불필요 — sas7bdat는 파일 내 컬럼 메타데이터 사용
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
| `EXPORT_SHEETS` | `dict` | `{테이블명: 시트명 or dict}` — 없으면 export 생략. dict 사용 시 `sql`, `columns`, `where`, `order_by`, `limit` 지원 |

### JOB 파일에서 사용 가능한 유틸

```python
from sas_to_duckdb import sql, table_exists

# SQL 실행 + 건수 로깅
sql(con, "라벨", "CREATE OR REPLACE TABLE ...")

# 테이블 존재 여부 확인
if table_exists(con, "some_table"):
    ...
```

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
| LOGIC | `sa11` → `sa12` → `sa20` → `sa_{YYYYMM}`, `TEMP01` (이연), `out_SA000`, `out_SA001` |
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

- 첫 번째 시트: **요약** (테이블명·시트명·건수·소요시간)
- `out_` 접두사 테이블은 자동으로 시트에 포함
- 시트명은 31자에서 자동 잘림 (openpyxl 제한)

---

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| `파일 없음: xxx.*` | data/YYYYMM/ 폴더에 해당 파일 없음 | 파일명·경로 확인, 압축 형식 확인 |
| `JOB 파일에 'NAME' 없음` | JOB 파일에 필수 항목 누락 | NAME, TABLES, logic, validate 확인 |
| `ey_a2601 없음` | 이연 계산용 테이블 미등록 | TABLES에 ey_a2601 추가하거나 별도 JOB에서 로드 |
| 인코딩 에러 | cp949/utf-8 모두 실패 | dat_loader.py의 ENCODINGS에 인코딩 추가 |
| 이전 JOB 결과 참조 실패 | 앞선 JOB을 실행하지 않음 | 앞 JOB 먼저 실행 (DB에 데이터 있어야 함) |
