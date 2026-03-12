"""
dat_loader.py
══════════════════════════════════════════════════════════════
유형A (고정폭 fixed-width) / 유형B (파이프 구분자) DAT 파일
표준 변환 유틸리티

단독 실행 예시:
    python dat_loader.py --file data/202601/BS100_202601.DAT --type pipe
    python dat_loader.py --file data/202601/fioBtLtrJ841_01_202601.DAT --type fwf --cols cols_fio841.json
    python dat_loader.py --help
══════════════════════════════════════════════════════════════
"""

import gzip
import zipfile
import json
import argparse
import logging
import pandas as pd
from io import TextIOWrapper
from pathlib import Path

try:
    import pyreadstat
    _HAS_PYREADSTAT = True
except ImportError:
    _HAS_PYREADSTAT = False

# 인코딩 fallback 순서
ENCODINGS = ["cp949", "utf-8"]

# sas_to_duckdb.py 와 함께 쓸 때는 그쪽 logger 를 그대로 사용
# 단독 실행 시에는 기본 logger 사용
log = logging.getLogger("pipeline")


# ══════════════════════════════════════════════
# 파일 열기 (압축 자동 감지)
# ══════════════════════════════════════════════

def open_file(path: Path, encoding: str):
    """
    확장자에 따라 알맞은 파일 객체 반환

    지원 포맷:
      .zip        → zip 내부 첫 번째 파일을 꺼내서 반환
      .gz         → gzip 스트림으로 열기
      .DAT / 그 외 → 일반 텍스트로 열기
    """
    suffix = path.suffix.lower()

    if suffix == ".zip":
        zf = zipfile.ZipFile(path)
        inner = zf.namelist()[0]
        log.debug(f"zip 내부 파일: {inner}")
        return TextIOWrapper(zf.open(inner), encoding=encoding)

    elif suffix == ".gz":
        return gzip.open(path, "rt", encoding=encoding)

    else:
        # .DAT / .dat / .csv / .prn 모두 일반 텍스트로 열기
        return open(path, "r", encoding=encoding)


# ══════════════════════════════════════════════
# 인코딩 자동 감지 래퍼
# ══════════════════════════════════════════════

def try_read(read_fn, path: Path, encodings: list = None, **kwargs) -> tuple[pd.DataFrame, str]:
    """
    encodings 순서대로 시도 → 성공한 (DataFrame, encoding) 반환
    모두 실패 시 마지막 UnicodeError를 raise

    Args:
        read_fn   : pd.read_fwf 또는 pd.read_csv
        path      : 파일 경로 (Path)
        encodings : 시도할 인코딩 목록 (기본값: 모듈 ENCODINGS)
        **kwargs  : read_fn 에 전달할 파라미터

    Returns:
        (df, 성공한_encoding)
    """
    encodings = encodings or ENCODINGS
    last_err  = None

    for enc in encodings:
        try:
            f  = open_file(path, enc)
            df = read_fn(f, **kwargs)
            f.close()
            log.debug(f"인코딩: {enc}")
            return df, enc
        except (UnicodeDecodeError, UnicodeError) as e:
            log.debug(f"인코딩 {enc} 실패 → 다음 시도")
            last_err = e

    raise last_err


# ══════════════════════════════════════════════
# 유형A : 고정폭(fixed-width) 읽기
# ══════════════════════════════════════════════

def read_fwf_dat(
    path      : Path,
    col_defs  : list,
    numeric   : list = None,
    encodings : list = None,
) -> pd.DataFrame:
    """
    유형A 고정폭 DAT 파일 읽기 (벡터 슬라이싱 방식)

    SAS 변환 규칙:
        @N  col  $W.  →  colspecs = (N-1, N-1+W),  name = "col"
        @N  col   W.  →  동일 (숫자형도 위치 규칙은 같음)

    Args:
        path     : 파일 경로 (.DAT / .zip / .gz 모두 가능)
        col_defs : [(시작, 끝), "컬럼명"] 형식의 리스트
                   예) [((0, 6), "CLS_YYMM"), ((16, 32), "PLYNO"), ...]
        numeric  : 숫자형으로 캐스팅할 컬럼명 리스트 (선택)
        encodings: 시도할 인코딩 목록 (기본값: 모듈 ENCODINGS)

    Returns:
        DataFrame (모든 컬럼 str, numeric 지정 컬럼은 float)
    """
    colspecs = [c[0] for c in col_defs]
    names    = [c[1] for c in col_defs]
    encodings = encodings or ENCODINGS
    numeric = numeric or []

    # 파일 전체를 한 컬럼(Series)으로 읽은 뒤 str 슬라이싱 → pd.read_fwf 대비 3~5배 빠름
    lines = None
    for enc in encodings:
        try:
            f = open_file(path, enc)
            raw = f.read()
            f.close()
            lines = raw.splitlines()
            break
        except (UnicodeDecodeError, UnicodeError):
            log.debug(f"인코딩 {enc} 실패 → 다음 시도")
    if lines is None:
        raise UnicodeDecodeError("all", b"", 0, 1, "모든 인코딩 실패")

    s = pd.Series(lines, dtype=str)
    data = {name: s.str[start:end] for (start, end), name in zip(colspecs, names)}
    df = pd.DataFrame(data)

    df = df.fillna("")
    df = _strip_str(df, exclude=numeric)
    df = _cast_numeric(df, numeric)
    return df


def read_fwf_duckdb(con, path: Path, col_defs: list, numeric: list = None):
    """
    DuckDB 네이티브 FWF 읽기 — pandas 우회, C++ 엔진으로 직접 처리

    con       : DuckDB 커넥션
    path      : 파일 경로 (.DAT / .gz — .zip은 미지원, 폴백 필요)
    col_defs  : [(시작, 끝), "컬럼명"] 리스트
    numeric   : 숫자형 캐스팅 컬럼명 리스트

    Returns: 건수 (int)
    """
    numeric_set = set(numeric or [])

    # 파일을 줄 단위 단일 컬럼으로 읽기
    # delim='\x01' (SOH): 데이터에 없는 구분자 → 줄 전체가 column0
    # encoding: utf-8 먼저 시도, 실패 시 euc-kr 계열 순서로 폴백
    for enc in ["utf-8", "euc-kr", "windows-949", "ms949"]:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _fwf_raw AS
                SELECT column0 AS line
                FROM read_csv('{path}',
                    delim      = '\x01',
                    header     = false,
                    encoding   = '{enc}',
                    columns    = {{'column0': 'VARCHAR'}})
            """)
            break
        except Exception:
            continue
    else:
        raise RuntimeError(f"DuckDB read_csv 인코딩 실패: {path}")

    # SUBSTR로 컬럼 추출 (SQL 1-based → start+1)
    exprs = []
    for (start, end), name in col_defs:
        width = end - start
        base = f"TRIM(SUBSTR(line, {start + 1}, {width}))"
        if name in numeric_set:
            base = f"TRY_CAST({base} AS DOUBLE)"
        exprs.append(f"{base} AS {name}")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _fwf_parsed AS
        SELECT {', '.join(exprs)} FROM _fwf_raw
    """)
    cnt = con.execute("SELECT COUNT(*) FROM _fwf_parsed").fetchone()[0]
    con.execute("DROP TABLE IF EXISTS _fwf_raw")
    return cnt


# ══════════════════════════════════════════════
# 유형B : 파이프 구분자 읽기
# ══════════════════════════════════════════════

def read_pipe_dat(
    path      : Path,
    col_names : list,
    numeric   : list = None,
    encodings : list = None,
    delimiter : str  = "|",
) -> pd.DataFrame:
    """
    유형B 파이프(|) 구분자 DAT 파일 읽기

    SAS 변환 규칙:
        informat col $W.     → col_names 에 순서대로 추가
        informat col BEST20. → 동일 (숫자형은 numeric 파라미터로 지정)

    Args:
        path      : 파일 경로 (.DAT / .zip / .gz 모두 가능)
        col_names : 컬럼명 순서 리스트
                    예) ["SLPDT", "CLS_DA_SEQNO", "NTACC_CD", ...]
        numeric   : 숫자형으로 캐스팅할 컬럼명 리스트 (선택)
        encodings : 시도할 인코딩 목록 (기본값: 모듈 ENCODINGS)
        delimiter : 구분자 (기본값: "|")

    Returns:
        DataFrame (모든 컬럼 str, numeric 지정 컬럼은 float)
    """
    df, _ = try_read(
        pd.read_csv, path,
        encodings=encodings,
        sep=delimiter,
        names=col_names,
        dtype=str,
        header=None,
        on_bad_lines="warn",
    )

    df = df.fillna("")
    df = _strip_str(df, exclude=numeric or [])
    df = _cast_numeric(df, numeric or [])
    return df


def read_pipe_duckdb(con, path: Path, col_names: list, numeric: list = None,
                     delimiter: str = "|"):
    """
    DuckDB 네이티브 파이프 구분자 읽기 — pandas 우회, C++ 엔진으로 직접 처리

    con       : DuckDB 커넥션
    path      : 파일 경로 (.DAT / .gz)
    col_names : 컬럼명 순서 리스트
    numeric   : 숫자형 캐스팅 컬럼명 리스트
    delimiter : 구분자 (기본값: "|")

    Returns: 건수 (int)
    """
    numeric_set = set(numeric or [])

    # 모든 컬럼을 VARCHAR로 읽기 (+더미: 끝에 구분자가 하나 더 있는 파일 대응)
    cols = [f"'column{i:02d}': 'VARCHAR'" for i in range(len(col_names))]
    cols.append(f"'_dummy': 'VARCHAR'")
    columns_def = ", ".join(cols)

    for enc in ["utf-8", "euc-kr", "windows-949", "ms949"]:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _pipe_raw AS
                SELECT * FROM read_csv('{path}',
                    delim        = '{delimiter}',
                    header       = false,
                    encoding     = '{enc}',
                    null_padding = true,
                    quote        = '',
                    columns      = {{{columns_def}}})
            """)
            break
        except Exception:
            continue
    else:
        raise RuntimeError(f"DuckDB read_csv 인코딩 실패: {path}")

    # 컬럼 리네임 + numeric 캐스팅
    exprs = []
    for i, name in enumerate(col_names):
        base = f"TRIM(column{i:02d})"
        if name in numeric_set:
            base = f"TRY_CAST({base} AS DOUBLE)"
        exprs.append(f"{base} AS {name}")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _pipe_parsed AS
        SELECT {', '.join(exprs)} FROM _pipe_raw
    """)
    cnt = con.execute("SELECT COUNT(*) FROM _pipe_parsed").fetchone()[0]
    con.execute("DROP TABLE IF EXISTS _pipe_raw")
    return cnt


# ══════════════════════════════════════════════
# 유형C : sas7bdat 읽기
# ══════════════════════════════════════════════

def read_sas7bdat_file(
    path     : Path,
    numeric  : list = None,
    encoding : str  = "cp949",
) -> pd.DataFrame:
    """
    유형C sas7bdat 파일 읽기

    - 헤더(컬럼명)는 파일 내부 메타데이터에서 자동 추출
    - cols 정의 불필요
    - pyreadstat 설치 필요: pip install pyreadstat

    Args:
        path     : .sas7bdat 파일 경로
        numeric  : 추가로 숫자형 캐스팅할 컬럼명 (대부분 자동 감지됨)
        encoding : 문자 인코딩 (기본: cp949)

    Returns:
        DataFrame (헤더 포함, 숫자형 컬럼은 자동 감지)
    """
    if not _HAS_PYREADSTAT:
        raise ImportError(
            "sas7bdat 읽기에는 pyreadstat 가 필요합니다.\n"
            "설치: pip install pyreadstat"
        )

    df, meta = pyreadstat.read_sas7bdat(
        str(path),
        encoding=encoding,
    )

    log.debug(f"sas7bdat 컬럼: {list(df.columns)}")
    log.debug(f"sas7bdat 라벨: {meta.column_labels}")

    # pyreadstat 이 자동으로 숫자형 감지하지만 추가 캐스팅 옵션도 지원
    df = _strip_str(df, exclude=numeric or [])
    if numeric:
        df = _cast_numeric(df, numeric)

    return df


# ══════════════════════════════════════════════
# 공통 유틸
# ══════════════════════════════════════════════

def _strip_str(df: pd.DataFrame, exclude: list = None) -> pd.DataFrame:
    """문자형(object) 컬럼 전체 앞뒤 공백 제거
    exclude : strip 제외할 컬럼명 (숫자형 캐스팅 대상 등)
    """
    exclude = set(exclude or [])
    for col in df.select_dtypes(include="object").columns:
        if col not in exclude:
            df[col] = df[col].str.strip()
    return df


def _cast_numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """지정 컬럼을 numeric으로 변환 (변환 불가 값은 NaN)"""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].str.strip() if df[col].dtype == object else df[col], errors="coerce")
    return df


def preview(df: pd.DataFrame, n: int = 5):
    """DataFrame 간단 미리보기"""
    log.info(f"shape : {df.shape[0]:,}행  ×  {df.shape[1]}열")
    log.info(f"컬럼  : {list(df.columns)}")
    log.info("\n" + df.head(n).to_string())


# ══════════════════════════════════════════════
# CLI (단독 실행용)
# ══════════════════════════════════════════════

def _cli():
    parser = argparse.ArgumentParser(
        description="DAT 파일 단독 읽기 테스트 (유형A/B 자동)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
예시:
  # 유형B (파이프): 컬럼명 자동 추론 (헤더 없으면 col_0, col_1...)
  python dat_loader.py --file data/202601/BS100_202601.zip --type pipe

  # 유형A (고정폭): 컬럼 정의를 JSON 파일로 전달
  python dat_loader.py --file data/202601/fio841.DAT --type fwf --cols cols_fio841.json

  # JSON 형식 (cols_fio841.json):
  # [[[0, 6], "CLS_YYMM"], [[16, 32], "PLYNO"], ...]
        """
    )
    parser.add_argument("--file",  "-f", required=True, help="읽을 DAT 파일 경로")
    parser.add_argument("--type",  "-t", choices=["fwf", "pipe"], required=True,
                        help="유형A=fwf / 유형B=pipe")
    parser.add_argument("--cols",  "-c", default=None,
                        help="컬럼 정의 JSON 파일 (fwf 필수, pipe 선택)")
    parser.add_argument("--numeric", "-n", nargs="*", default=[],
                        help="숫자형 캐스팅할 컬럼명")
    parser.add_argument("--rows",  "-r", type=int, default=5,
                        help="미리보기 행 수 (기본값: 5)")
    args = parser.parse_args()

    path = Path(args.file)

    if args.type == "fwf":
        if not args.cols:
            parser.error("--type fwf 사용 시 --cols JSON 파일이 필요합니다")
        raw      = json.loads(Path(args.cols).read_text())
        col_defs = [((c[0][0], c[0][1]), c[1]) for c in raw]
        df       = read_fwf_dat(path, col_defs, numeric=args.numeric)

    else:  # pipe
        col_names = None
        if args.cols:
            col_names = json.loads(Path(args.cols).read_text())
        # 컬럼명 없으면 일단 읽고 자동 컬럼명 사용
        if col_names:
            df = read_pipe_dat(path, col_names, numeric=args.numeric)
        else:
            f  = open_file(path, ENCODINGS[0])
            df = pd.read_csv(f, sep="|", dtype=str, header=None)
            f.close()
            log.warning("--cols 없음 → 자동 컬럼명(col_0, col_1, ...) 사용")

    preview(df, args.rows)


if __name__ == "__main__":
    _cli()
