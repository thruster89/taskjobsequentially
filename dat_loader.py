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
import shutil
import tempfile
import zipfile
import json
import argparse
import logging
import time
import pandas as pd
from io import TextIOWrapper
from pathlib import Path
from charset_normalizer import from_bytes

try:
    import pyreadstat
    _HAS_PYREADSTAT = True
except ImportError:
    _HAS_PYREADSTAT = False

# 인코딩 fallback 순서
ENCODINGS = ["cp949", "utf-8"]
# DuckDB 네이티브 읽기용 인코딩 순서
# encodings 확장 설치 시 cp949 직접 지원, 없으면 euc_kr 폴백
DUCKDB_ENCODINGS = ["utf-8", "cp949", "euc_kr"]


# charset_normalizer 결과 → DuckDB 호환 인코딩 매핑
# Big5(중국어 번체)는 cp949와 바이트 범위가 겹쳐서 한국어 파일을 오탐하는 경우가 많음
_ENC_MAP = {
    "euc-kr": "euc_kr", "euckr": "euc_kr", "cp949": "euc_kr",
    "big5": "euc_kr", "big5hkscs": "euc_kr",   # 한국어 오탐 보정
    # latin1(iso-8859-1)은 0x00-0xFF 모든 바이트를 허용하므로
    # charset_normalizer가 "판별 불가"일 때 latin1을 반환하는 경우가 많음.
    # 한국어 파일에서는 거의 항상 cp949/euc_kr이므로 euc_kr로 매핑하여
    # DuckDB 폴백(파일 다중 읽기) 병목을 방지한다.
    "latin-1": "euc_kr", "latin1": "euc_kr",
    "iso-8859-1": "euc_kr", "iso88591": "euc_kr",
    "windows-1252": "euc_kr", "windows1252": "euc_kr",
    "cp1252": "euc_kr",
    "utf-8": "utf-8", "ascii": "utf-8", "utf8": "utf-8",
}

def _detect_file_encoding(path, sample_bytes=256_000):
    """
    파일 앞부분 바이트를 읽어서 인코딩을 감지한다.
    charset_normalizer 사용 — DuckDB SQL 실행 없이 순수 바이트 분석.

    Returns: (감지된 인코딩, 신뢰도)
    """
    f = open_file_binary(path)
    try:
        raw = f.read(sample_bytes)
    finally:
        f.close()

    result = from_bytes(raw)
    best = result.best()
    if best is None:
        return "utf-8", 0.0
    detected = best.encoding.lower().replace("-", "").replace("_", "")
    # 정규화
    for key, val in _ENC_MAP.items():
        if detected == key.replace("-", "").replace("_", ""):
            return val, best.encoding  # (DuckDB 호환 이름, 원본 감지명)
    return best.encoding, best.encoding


def _detect_duckdb_encoding(con, path, enc_list=None):
    """
    1차: charset_normalizer로 바이트 기반 감지 (빠름)
    2차: 실패 시 DuckDB LIMIT 100 폴백 (기존 방식)
    """
    enc_list = enc_list or DUCKDB_ENCODINGS

    # 1차: 바이트 기반 감지
    try:
        detected, raw_name = _detect_file_encoding(path)
        # DuckDB에서 지원하는 인코딩인지 확인
        if detected in enc_list or detected.replace("-", "_") in enc_list:
            log.info(f"    인코딩 자동감지: {raw_name} → {detected}")
            return detected
        # 감지 결과가 enc_list에 없으면 매핑 시도
        normalized = detected.lower().replace("-", "_")
        for enc in enc_list:
            if enc.lower().replace("-", "_") == normalized:
                log.info(f"    인코딩 자동감지: {raw_name} → {enc}")
                return enc
        log.info(f"    인코딩 자동감지 결과({raw_name} → {detected})가 DuckDB 미지원 → DuckDB 폴백 감지")
    except Exception as e:
        log.info(f"    인코딩 자동감지 실패({e}) → DuckDB 폴백 감지")

    # 2차: DuckDB LIMIT 100 폴백
    for enc in enc_list:
        try:
            con.execute(f"""
                SELECT column0 FROM read_csv('{path}',
                    delim    = '\x01',
                    header   = false,
                    encoding = '{enc}',
                    columns  = {{'column0': 'VARCHAR'}})
                LIMIT 100
            """).fetchall()
            return enc
        except Exception:
            continue
    raise RuntimeError(f"DuckDB 인코딩 감지 실패 (시도: {enc_list}): {path}")

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


def open_file_binary(path: Path):
    """
    확장자에 따라 바이너리 파일 객체 반환 (고정폭 바이트 슬라이싱용)

    지원 포맷:
      .zip → zip 내부 첫 번째 파일
      .gz  → gzip 바이너리 스트림
      그 외 → 일반 바이너리 열기
    """
    suffix = path.suffix.lower()

    if suffix == ".zip":
        zf = zipfile.ZipFile(path)
        inner = zf.namelist()[0]
        log.debug(f"zip 내부 파일: {inner}")
        return zf.open(inner)

    elif suffix == ".gz":
        return gzip.open(path, "rb")

    else:
        return open(path, "rb")


def decompress_gz(path: Path):
    """
    .gz 파일을 해제 + cp949→utf-8 변환하여 .utf8.dat 로 반환.
    DuckDB encodings 확장 없이 utf-8 네이티브로 읽을 수 있게 함.
    이미 변환된 파일이 gz보다 새로우면 스킵.
    gz가 아닌 비-utf8 파일도 변환 대상.
    """
    # 출력 파일명 결정
    if path.suffix.lower() == ".gz":
        if path.name.lower().endswith(".dat.gz"):
            base = path.with_name(path.name[:-3])  # .dat.gz → .dat
        else:
            base = path.with_suffix("")
    else:
        base = path
    out = base.with_suffix(".utf8.dat")

    # 이미 변환된 파일이 원본보다 새로우면 스킵
    if out.exists() and out.stat().st_mtime >= path.stat().st_mtime:
        log.info(f"    변환 캐시 사용: {out.name} ({out.stat().st_size / 1024**2:.0f}MB)")
        return out

    # 인코딩 감지
    detected, raw_name = _detect_file_encoding(path)
    # DuckDB 네이티브 utf-8이면 gz 해제만
    is_utf8 = detected.lower().replace("-", "").replace("_", "") in ("utf8", "ascii")

    log.info(f"    gz 해제 + 인코딩 변환 시작: {path.name} ({raw_name} → utf-8)")
    t0 = time.time()

    if path.suffix.lower() == ".gz":
        fin_raw = gzip.open(path, "rb")
    else:
        fin_raw = open(path, "rb")

    PROGRESS_INTERVAL = 5_000_000  # 500만행마다 진행 로그
    BUF_SIZE = 16 * 1024 * 1024     # 16MB 청크 (성능 유지)

    try:
        if is_utf8:
            # utf-8이면 청크 복사 + 행 수 카운트
            line_count = 0
            last_reported = 0
            with open(out, "wb") as fout:
                while True:
                    chunk = fin_raw.read(BUF_SIZE)
                    if not chunk:
                        break
                    fout.write(chunk)
                    line_count += chunk.count(b"\n")
                    if line_count // PROGRESS_INTERVAL > last_reported:
                        last_reported = line_count // PROGRESS_INTERVAL
                        log.info(f"    변환 진행: {line_count:>12,}행  ({time.time()-t0:.1f}초)")
        else:
            # cp949/euc-kr → utf-8 변환 (청크 단위 + 행 수 카운트)
            src_enc = "cp949"  # euc_kr 상위호환
            line_count = 0
            last_reported = 0
            remainder = b""
            with open(out, "wb") as fout:
                while True:
                    chunk = fin_raw.read(BUF_SIZE)
                    if not chunk:
                        if remainder:
                            fout.write(remainder.decode(src_enc, errors="replace").encode("utf-8"))
                            line_count += 1
                        break
                    data = remainder + chunk
                    # 마지막 줄이 잘릴 수 있으므로 마지막 \n 이후는 다음 청크로
                    last_nl = data.rfind(b"\n")
                    if last_nl == -1:
                        remainder = data
                        continue
                    to_write = data[:last_nl + 1]
                    remainder = data[last_nl + 1:]
                    fout.write(to_write.decode(src_enc, errors="replace").encode("utf-8"))
                    line_count += to_write.count(b"\n")
                    if line_count // PROGRESS_INTERVAL > last_reported:
                        last_reported = line_count // PROGRESS_INTERVAL
                        log.info(f"    변환 진행: {line_count:>12,}행  ({time.time()-t0:.1f}초)")
    finally:
        fin_raw.close()

    sz = out.stat().st_size / 1024 ** 2
    log.info(f"    변환 완료: {out.name} ({sz:.0f}MB, {time.time()-t0:.1f}초)")
    return out


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
    encoding  : str  = "cp949",
) -> pd.DataFrame:
    """
    유형A 고정폭 DAT/PRN 파일 읽기 (바이트 슬라이싱 방식)

    바이트 단위로 슬라이싱한 뒤 컬럼별로 디코딩하므로
    cp949 한글(2바이트)이 포함되어도 위치가 밀리지 않음.

    SAS 변환 규칙:
        @N  col  $W.  →  colspecs = (N-1, N-1+W),  name = "col"
        @N  col   W.  →  동일 (숫자형도 위치 규칙은 같음)

    Args:
        path     : 파일 경로 (.DAT / .prn / .zip / .gz 모두 가능)
        col_defs : [(시작, 끝), "컬럼명"] 형식의 리스트
                   예) [((0, 6), "CLS_YYMM"), ((16, 32), "PLYNO"), ...]
        numeric  : 숫자형으로 캐스팅할 컬럼명 리스트 (선택)
        encoding : 디코딩 인코딩 (기본값: cp949)

    Returns:
        DataFrame (모든 컬럼 str, numeric 지정 컬럼은 float)
    """
    colspecs = [c[0] for c in col_defs]
    names    = [c[1] for c in col_defs]
    numeric = numeric or []

    # 바이트 단위로 줄 단위 읽기 (청크 단위, 전체 메모리 적재 방지)
    CHUNK = 200_000
    chunks = []
    f = open_file_binary(path)
    try:
        batch = []
        for raw_line in f:
            line = raw_line.rstrip(b"\r\n")
            if not line:
                continue
            row = tuple(
                line[start:end].decode(encoding, errors="replace").strip()
                for (start, end) in colspecs
            )
            batch.append(row)
            if len(batch) >= CHUNK:
                chunk = pd.DataFrame(batch, columns=names)
                chunk = chunk.fillna("")
                chunk = _cast_numeric(chunk, numeric)
                chunks.append(chunk)
                batch = []
        if batch:
            chunk = pd.DataFrame(batch, columns=names)
            chunk = chunk.fillna("")
            chunk = _cast_numeric(chunk, numeric)
            chunks.append(chunk)
    finally:
        f.close()

    df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=names)
    return df


def read_fwf_duckdb(con, path: Path, col_defs: list, numeric: list = None,
                    encodings: list = None):
    """
    DuckDB 네이티브 FWF 읽기 — pandas 우회, C++ 엔진으로 직접 처리

    con       : DuckDB 커넥션
    path      : 파일 경로 (.DAT / .gz — .zip은 미지원, 폴백 필요)
    col_defs  : [(시작, 끝), "컬럼명"] 리스트
    numeric   : 숫자형 캐스팅 컬럼명 리스트
    encodings : 시도할 인코딩 목록 (기본값: DUCKDB_ENCODINGS)

    Returns: 건수 (int)
    """
    numeric_set = set(numeric or [])
    enc_list = encodings or DUCKDB_ENCODINGS

    # 0단계: gz 해제 + cp949→utf-8 변환 (DuckDB 멀티스레드 + 네이티브 인코딩)
    path = decompress_gz(path)
    enc = "utf-8"  # decompress_gz가 utf-8로 변환 완료

    # 1단계: 전체 파일 읽기
    t1 = time.time()
    log.info(f"    전체 읽기 시작 (FWF, enc={enc})")
    remaining = []
    for try_enc in [enc]:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _fwf_raw AS
                SELECT column0 AS line
                FROM read_csv('{path}',
                    delim      = '\x01',
                    header     = false,
                    encoding   = '{try_enc}',
                    columns    = {{'column0': 'VARCHAR'}})
            """)
            if try_enc != enc:
                log.info(f"    인코딩 재시도 성공: {enc} → {try_enc}")
            break
        except Exception as e:
            if try_enc == remaining[-1] if remaining else try_enc == enc:
                raise
            log.info(f"    인코딩 {try_enc} 전체 읽기 실패, 재시도: {e}")
            continue
    log.info(f"    전체 읽기 완료  ({time.time()-t1:.1f}초)")

    # 3단계: SUBSTR로 컬럼 추출 (SQL 1-based → start+1)
    t2 = time.time()
    log.info(f"    컬럼 파싱 시작 (FWF, {len(col_defs)}개 컬럼)")
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
    log.info(f"    컬럼 파싱 완료  {cnt:,}건  ({time.time()-t2:.1f}초)")
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
                     delimiter: str = "|", encodings: list = None,
                     fast: bool = None):
    """
    DuckDB 네이티브 파이프 구분자 읽기 — pandas 우회, C++ 엔진으로 직접 처리

    con       : DuckDB 커넥션
    path      : 파일 경로 (.DAT / .gz)
    col_names : 컬럼명 순서 리스트
    numeric   : 숫자형 캐스팅 컬럼명 리스트
    delimiter : 구분자 (기본값: "|")
    encodings : 시도할 인코딩 목록 (기본값: DUCKDB_ENCODINGS)
    fast      : None(기본) → .gz 파일이면 자동 적용, .DAT이면 미적용.
                True → 강제 적용, False → 강제 미적용.
                gz 해제 + cp949→utf-8 사전 변환 후 utf-8 네이티브 읽기.
                인코딩 감지·euc_kr 디코딩 오버헤드를 건너뛰어 빨라짐.

    Returns: 건수 (int)
    """
    numeric_set = set(numeric or [])
    enc_list = encodings or DUCKDB_ENCODINGS

    # ── fast 모드: gz 해제 + utf-8 변환 → 인코딩 감지 스킵 ──
    # fast=True 명시 또는 .gz 파일이면 자동 적용 (fast=False 명시 시 제외)
    use_fast = fast if fast is not None else str(path).endswith('.gz')
    if use_fast:
        path = decompress_gz(path)
        enc = "utf-8"
        log.info(f"    fast 모드: utf-8 사전 변환 완료 → 인코딩 감지 스킵")
    else:
        # 1단계: 인코딩 감지
        t0 = time.time()
        enc = _detect_duckdb_encoding(con, path, enc_list)
        log.info(f"    인코딩 감지: {enc}  ({time.time()-t0:.1f}초)")

    # 2단계: 실제 컬럼 수 감지 → 전부 읽은 뒤 필요한 것만 SELECT
    t1 = time.time()
    n_cols = len(col_names)

    # 샘플 1줄로 실제 파이프 수(=컬럼 수) 파악
    actual_cols = n_cols
    try:
        # 바이트 기반: 압축/인코딩 무관하게 첫 줄 읽기
        f = open_file_binary(path)
        try:
            first_line = f.readline(1024 * 1024)  # 최대 1MB
        finally:
            f.close()
        if first_line:
            delim_byte = delimiter.encode('ascii')
            actual_cols = first_line.count(delim_byte) + 1
            log.info(f"    샘플 파이프 수: {actual_cols - 1}개 → 컬럼 {actual_cols}개")
    except Exception as e:
        log.info(f"    샘플 컬럼 수 감지 실패({e}), 정의 {n_cols}개 사용")

    # 실제 컬럼 수만큼 columns 정의 (부족하면 ignore_errors 발생 방지)
    total_cols = max(actual_cols, n_cols)
    col_map = ", ".join(f"column{i:02d}: 'VARCHAR'" for i in range(total_cols))
    if actual_cols != n_cols:
        log.info(f"    실제 컬럼 {actual_cols}개, 정의 {n_cols}개 → {total_cols}개로 읽기")
    log.info(f"    전체 읽기 시작 (PIPE direct, enc={enc}, {total_cols}개 컬럼)")

    remaining = [e for e in enc_list if e != enc]
    for try_enc in [enc] + remaining:
        try:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _pipe_raw AS
                SELECT * FROM read_csv('{path}',
                    delim        = '{delimiter}',
                    header       = false,
                    encoding     = '{try_enc}',
                    null_padding = true,
                    strict_mode  = false,
                    columns      = {{{col_map}}})
            """)
            if try_enc != enc:
                log.info(f"    인코딩 재시도 성공: {enc} → {try_enc}")
            break
        except Exception as e:
            if try_enc == remaining[-1] if remaining else try_enc == enc:
                raise
            log.info(f"    인코딩 {try_enc} 전체 읽기 실패, 재시도: {e}")
            continue
    log.info(f"    전체 읽기 완료  ({time.time()-t1:.1f}초)")

    # 3단계: 필요한 컬럼만 SELECT + 리네임 + 숫자 캐스팅
    t2 = time.time()
    log.info(f"    컬럼 파싱 시작 (PIPE, {n_cols}/{total_cols}개 컬럼 추출)")
    exprs = []
    for i, name in enumerate(col_names):
        src = f"column{i:02d}"
        base = f"TRIM({src})"
        if name in numeric_set:
            base = f"TRY_CAST({base} AS DOUBLE)"
        exprs.append(f"{base} AS {name}")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _pipe_parsed AS
        SELECT {', '.join(exprs)} FROM _pipe_raw
    """)
    cnt = con.execute("SELECT COUNT(*) FROM _pipe_parsed").fetchone()[0]
    con.execute("DROP TABLE IF EXISTS _pipe_raw")
    log.info(f"    컬럼 파싱 완료  {cnt:,}건  ({time.time()-t2:.1f}초)")
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
# 유형D : CSV 읽기
# ══════════════════════════════════════════════

def read_csv_file(
    path      : Path,
    col_names : list = None,
    numeric   : list = None,
    encodings : list = None,
    delimiter : str  = ",",
    header    : bool = False,
) -> pd.DataFrame:
    """
    CSV 파일 읽기 (pandas)

    Args:
        path      : 파일 경로
        col_names : 컬럼명 리스트 (None이면 header=True 필요)
        numeric   : 숫자형 캐스팅 컬럼명 리스트
        encodings : 인코딩 fallback 순서
        delimiter : 구분자 (기본값: ",")
        header    : 첫 행을 헤더로 사용할지 여부

    Returns:
        DataFrame (모든 컬럼 str, numeric 지정 컬럼은 float)
    """
    kw = dict(sep=delimiter, dtype=str, on_bad_lines="warn")
    if col_names:
        kw["names"] = col_names
        kw["header"] = 0 if header else None  # header=True면 첫행 스킵
    else:
        kw["header"] = 0 if header else None

    df, _ = try_read(pd.read_csv, path, encodings=encodings, **kw)
    df = df.fillna("")
    df = _strip_str(df, exclude=numeric or [])
    df = _cast_numeric(df, numeric or [])
    return df


def read_csv_duckdb(
    con, path: Path, col_names: list = None, numeric: list = None,
    delimiter: str = ",", header: bool = False, encodings: list = None,
    fast: bool = None,
):
    """
    DuckDB 네이티브 CSV 읽기 — read_csv 직접 사용

    fast : True/.gz 자동 → gz 해제 + cp949→utf-8 사전 변환 후 utf-8 네이티브 읽기.
    Returns: 건수 (int)
    """
    numeric_set = set(numeric or [])
    enc_list = encodings or DUCKDB_ENCODINGS
    hdr = "true" if header else "false"

    # fast 모드: gz 해제 + utf-8 변환 → 인코딩 감지 스킵
    # fast=True 명시 또는 .gz 파일이면 자동 적용
    use_fast = fast if fast is not None else str(path).endswith('.gz')
    if use_fast:
        path = decompress_gz(path)
        enc = "utf-8"
        log.info(f"    fast 모드: utf-8 사전 변환 완료 → 인코딩 감지 스킵")
    else:
        # 1단계: 샘플로 인코딩 감지
        t0 = time.time()
        enc = _detect_duckdb_encoding(con, path, enc_list)
        log.info(f"    인코딩 감지: {enc}  ({time.time()-t0:.1f}초)")

    # 2단계: 전체 파일 읽기 (인코딩 실패 시 나머지 인코딩으로 재시도)
    t1 = time.time()
    log.info(f"    전체 읽기 시작 (CSV, enc={enc})")
    remaining = [e for e in enc_list if e != enc]
    for try_enc in [enc] + remaining:
        try:
            if col_names:
                col_map = ", ".join(
                    f"'{c}': 'DOUBLE'" if c in numeric_set else f"'{c}': 'VARCHAR'"
                    for c in col_names
                )
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE _csv_parsed AS
                    SELECT * FROM read_csv('{path}',
                        delim        = '{delimiter}',
                        header       = {hdr},
                        encoding     = '{try_enc}',
                        null_padding = true,
                        strict_mode  = false,
                        columns      = {{{col_map}}})
                """)
            else:
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE _csv_parsed AS
                    SELECT * FROM read_csv('{path}',
                        delim    = '{delimiter}',
                        header   = true,
                        encoding = '{try_enc}',
                        all_varchar = true)
                """)
            if try_enc != enc:
                log.info(f"    인코딩 재시도 성공: {enc} → {try_enc}")
            break
        except Exception as e:
            if try_enc == remaining[-1] if remaining else try_enc == enc:
                raise
            log.info(f"    인코딩 {try_enc} 전체 읽기 실패, 재시도: {e}")
            continue
    log.info(f"    전체 읽기 완료  ({time.time()-t1:.1f}초)")

    # numeric 캐스팅 (col_names 없는 경우)
    if not col_names and numeric_set:
        cols_info = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='_csv_parsed' ORDER BY ordinal_position"
        ).fetchall()
        exprs = []
        for (c,) in cols_info:
            if c in numeric_set:
                exprs.append(f"TRY_CAST({c} AS DOUBLE) AS {c}")
            else:
                exprs.append(c)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _csv_parsed AS
            SELECT {', '.join(exprs)} FROM _csv_parsed
        """)

    cnt = con.execute("SELECT COUNT(*) FROM _csv_parsed").fetchone()[0]
    log.info(f"    CSV 파싱 완료  {cnt:,}건  ({time.time()-t1:.1f}초)")
    return cnt


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
