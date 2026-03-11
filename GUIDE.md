# SAS → DuckDB 사업비배분 파이프라인 사용자 가이드

## 개요

SAS에서 수행하던 사업비배분 JOB을 Python/DuckDB로 전환한 파이프라인입니다.

- **3개 JOB**이 순차 실행되며, 각 JOB은 `load → logic → validate → export` 단계를 거칩니다.
- **하나의 DB**(`pipeline.duckdb`)에 월별 데이터가 누적됩니다.

```
job1 (보험료 집계)          job2 (배분 결과)           job3 (마감 검증)
  load   fio841, fio843      load   sa01, sa02          load   erp, bs101, bs104, bs105
    ↓                          ↓                          ↓
  logic  BASE_DATA_CH 등      logic  sa11→sa20→집계      logic  (확장 가능)
    ↓                          ↓                          ↓
  validate 건수 확인          validate 시산표 비교        validate IBNR, TM/CM 체크
    ↓                          ↓                          ↓
  export  job1_YYYYMM.xlsx   export  job2_YYYYMM.xlsx   export  job3_YYYYMM.xlsx
```

---

## 요구사항

```bash
pip install duckdb pandas openpyxl
# sas7bdat 파일을 읽는 경우 추가:
pip install pyreadstat
```

| 파일 | 설명 |
|------|------|
| `sas_to_duckdb.py` | 메인 파이프라인 |
| `dat_loader.py` | DAT 파일 읽기 유틸리티 (fwf / pipe / sas7bdat) |

---

## 폴더 구조

```
프로젝트/
├── sas_to_duckdb.py
├── dat_loader.py
├── data/
│   ├── 202601/          ← --ym 202601 일 때 탐색 위치
│   │   ├── fioBtLtrJ841_01_202601.DAT   (또는 .zip / .dat.gz)
│   │   ├── fioBtLtrJ843_01_202601.DAT
│   │   ├── BS100_202601.DAT
│   │   ├── RS100_202601.DAT
│   │   ├── RS101_202601.DAT
│   │   ├── BS101_202601.DAT
│   │   ├── BS104_202601.DAT
│   │   └── BS105_202601.DAT
│   └── 202602/          ← --ym 202602 일 때
├── db/
│   └── pipeline.duckdb  ← 월별 데이터 누적
├── output/
│   ├── job1_202601.xlsx
│   ├── job2_202601.xlsx
│   └── job3_202601.xlsx
└── logs/
    └── 20260301_143000.log
```

파일 확장자는 자동 탐색됩니다: `.zip` → `.dat.gz` → `.DAT` → `.dat` → `.prn` → `.csv` → `.csv.gz` → `.sas7bdat`

---

## 실행 방법

### 전체 실행 (기본)

```bash
python sas_to_duckdb.py --ym 202601
```

job1 → job2 → job3 순차 실행됩니다.

### 특정 JOB만 실행

```bash
# job1만
python sas_to_duckdb.py --ym 202601 --job 1

# job2, job3만
python sas_to_duckdb.py --ym 202601 --job 2 3
```

### 로드 생략 (이미 적재된 데이터 사용)

```bash
python sas_to_duckdb.py --ym 202601 --job 2 --skip-load
```

DB에 이미 데이터가 있을 때, 로직/검증/출력만 재실행하고 싶은 경우 사용합니다.

### 목록 확인

```bash
python sas_to_duckdb.py --list
```

JOB별 테이블 구성과 설명을 출력합니다.

### DEBUG 로그

```bash
python sas_to_duckdb.py --ym 202601 -v
```

콘솔에 DEBUG 레벨 로그가 표시됩니다. 파일 로그(`logs/`)에는 항상 DEBUG가 기록됩니다.

---

## CLI 옵션 요약

| 옵션 | 단축 | 설명 | 기본값 |
|------|------|------|--------|
| `--ym YYYYMM` | | 처리할 년월 | `202601` |
| `--job N [N ...]` | `-j` | 실행할 JOB 번호 (1, 2, 3) | 전체 |
| `--skip-load` | | LOAD 단계 생략 | OFF |
| `--verbose` | `-v` | DEBUG 로그 콘솔 출력 | OFF |
| `--list` | `-l` | JOB·테이블 목록 출력 | |

---

## 월별 누적

하나의 DB 파일(`db/pipeline.duckdb`)에 월별 데이터가 쌓입니다.

```bash
# 1월 적재
python sas_to_duckdb.py --ym 202601

# 2월 적재 → 1월 데이터는 유지되고 2월이 추가됨
python sas_to_duckdb.py --ym 202602

# 1월 재적재 → 1월 데이터만 교체 (2월은 유지)
python sas_to_duckdb.py --ym 202601
```

각 테이블의 `month_col`(예: `CLS_YYMM`, `SLPDT`)을 기준으로 해당 월 데이터만 DELETE 후 INSERT합니다.

---

## JOB 구성

### JOB 1: 원천 데이터 집계 (보험료)

| 단계 | 내용 |
|------|------|
| LOAD | `fio841` (수입보험료), `fio843` (사업비배분 보유건수) |
| LOGIC | `BASE_DATA_CH` (841 보험료집계), `BASE_DATA_CH_843` (843 보험료집계), `BASE_DATA_CH2` (843 건수집계), `CH` (취급기관 이상체크) |
| VALIDATE | 각 테이블 건수 확인 |
| EXPORT | `job1_YYYYMM.xlsx` |

### JOB 2: 배분 결과 (SA 변환·집계)

| 단계 | 내용 |
|------|------|
| LOAD | `sa01` (유지비·신계약비 배분), `sa02` (손해조사비 배분) |
| LOGIC | `sa11` → `sa12` → `sa20` → `sa_{YYYYMM}` (최종 배분집계), `TEMP01` (이연 계산, ey_a2601 필요), `out_SA000` (순사업비 요약), `out_SA001` (신계약비·수금비 추세) |
| VALIDATE | 배분 결과 건수, 전표 vs 배분결과 시산표 비교 |
| EXPORT | `job2_YYYYMM.xlsx` |

### JOB 3: 마감 검증 (ERP·BS)

| 단계 | 내용 |
|------|------|
| LOAD | `erp` (ERP 전표), `bs101` (원수실적), `bs104` (지급보험금), `bs105` (지급준비금) |
| LOGIC | 확장 가능 (현재 pass) |
| VALIDATE | IBNR 담보코드 누락 (bs104, bs105), 자동차 TM/CM 비비례 배분 오류, 재마감 자동차 금액 확인 |
| EXPORT | `job3_YYYYMM.xlsx` |

---

## 테이블 등록 (새 파일 추가 시)

`TABLE_REGISTRY`에 항목을 추가합니다:

```python
"새테이블명": {
    "type": "pipe",                        # fwf / pipe / sas7bdat
    "file": "파일명_{YYYYMM}.DAT",         # {YYYYMM}은 자동 치환
    "desc": "설명",
    "month_col": "CLS_YYMM",              # 월별 누적 기준 컬럼 (없으면 전체 교체)
    "cols": ["COL1", "COL2", ...],         # pipe: 컬럼명 리스트
},
```

이후 `JOBS` 리스트에서 해당 JOB의 `tables`에 추가합니다:

```python
JOBS = [
    {
        "name": "job1",
        "tables": ["fio841", "fio843", "새테이블명"],   # ← 여기에 추가
        ...
    },
]
```

숫자형 컬럼이 있으면 `NUMERIC_COLS`에도 등록합니다:

```python
NUMERIC_COLS = {
    "새테이블명": ["AMT_COL1", "AMT_COL2"],
}
```

---

## 비즈니스 로직 수정

각 JOB의 로직은 독립된 함수입니다:

| 함수 | 위치 | 설명 |
|------|------|------|
| `logic_job1()` | 보험료 집계 SQL | BASE_DATA_CH 등 생성 |
| `logic_job2()` | SA 변환·집계 SQL | sa11→sa20→집계, 이연, 순사업비 |
| `logic_job3()` | 마감 로직 SQL | 현재 빈 함수 (필요 시 추가) |
| `validate_job1()` | job1 검증 | 건수 확인 |
| `validate_job2()` | job2 검증 | 시산표 비교 |
| `validate_job3()` | job3 검증 | IBNR, TM/CM 등 |

SQL 추가/수정은 해당 함수 안에서 `_exec(con, "라벨", "SQL")` 형태로 작성합니다.

---

## 출력 파일

각 JOB이 별도 Excel 파일을 생성합니다:

```
output/
├── job1_202601.xlsx   ← fio841, fio843, 집계 테이블
├── job2_202601.xlsx   ← sa01, sa02, out_SA000, out_SA001
└── job3_202601.xlsx   ← erp, bs101, bs104, bs105
```

각 Excel 파일의 첫 번째 시트는 **요약** 시트로, 테이블명·시트명·건수·소요시간이 정리됩니다. `out_` 접두사 테이블은 자동으로 시트에 포함됩니다.

---

## 문제 해결

| 증상 | 원인 | 해결 |
|------|------|------|
| `파일 없음: xxx.*` | data/YYYYMM/ 폴더에 해당 파일 없음 | 파일명·경로 확인, 압축 형식 확인 |
| `필수 테이블 없음` | 이전 JOB의 load를 안 했음 | 앞 JOB부터 실행하거나 `--skip-load` 제거 |
| `ey_a2601 없음` | 이연 계산용 테이블 미등록 | TABLE_REGISTRY에 ey_a2601 추가 |
| 인코딩 에러 | cp949/utf-8 모두 실패 | dat_loader.py의 ENCODINGS에 인코딩 추가 |
| Excel 시트명 31자 초과 | openpyxl 제한 | 자동으로 31자에서 잘림 |
