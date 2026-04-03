"""
런셋(RunSet) — 여러 JOB을 연속 실행 + 전체 타임아웃 + 예약 실행

사용법:
  python runset.py                          # runsets/ 폴더의 기본 런셋 실행
  python runset.py --config runsets/daily.py # 특정 런셋 실행
  python runset.py --config runsets/daily.py --at 06:00                # 06:00에 실행
  python runset.py --config runsets/daily.py --at "2026-04-04 06:00"  # 특정 일시에 실행
  python runset.py --config runsets/daily.py --at +30m                # 30분 후 실행
  python runset.py --config runsets/daily.py --at +2h                 # 2시간 후 실행
  python runset.py --config runsets/daily.py --timeout 7200   # 전체 2시간 제한

런셋 설정 파일 예시 (runsets/daily.py):

  YM = ["202603"]
  JOBS = [
      {"job": "jobs/job1.py"},
      {"job": "jobs/job2.py", "stage": ["load", "logic"]},
      {"job": "jobs/job3.py", "skip_load": True},
  ]
  TIMEOUT = 7200      # 전체 타임아웃(초), 0=무제한 (선택)
  LOAD_TIMEOUT = 3600 # 테이블당 타임아웃(초) (선택)
"""

import sys
import os
import time
import signal
import logging
import argparse
import importlib.util
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

log = logging.getLogger("runset")


def setup_logger():
    if log.handlers:
        return
    log.setLevel(logging.INFO)

    class AlignedFormatter(logging.Formatter):
        _WIDTH = len("[WARNING]")
        def format(self, record):
            bracket = f"[{record.levelname}]"
            record.bracket = f"{bracket:<{self._WIDTH}}"
            return super().format(record)

    fmt = AlignedFormatter(
        fmt="%(asctime)s %(bracket)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"runset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    log.info(f"로그 파일: {log_file}")


def load_runset(path):
    """런셋 설정 파일 로드"""
    spec = importlib.util.spec_from_file_location("_runset", path)
    mod = spec.loader.load_module()
    if not hasattr(mod, "YM"):
        raise AttributeError(f"런셋 파일에 'YM' 없음: {path}")
    if not hasattr(mod, "JOBS"):
        raise AttributeError(f"런셋 파일에 'JOBS' 없음: {path}")
    return mod


def wait_until(target_str):
    """
    지정 시각까지 대기.
    형식:
      06:00           → 오늘 06:00 (지났으면 내일)
      2026-04-04 06:00 → 특정 일시
      +30m            → 30분 후
      +2h             → 2시간 후
    """
    now = datetime.now()

    # +숫자m / +숫자h → 상대 시간
    if target_str.startswith("+"):
        val = target_str[1:]
        if val.endswith("m"):
            target = now + timedelta(minutes=int(val[:-1]))
        elif val.endswith("h"):
            target = now + timedelta(hours=int(val[:-1]))
        else:
            target = now + timedelta(minutes=int(val))
    elif " " in target_str:
        target = datetime.strptime(target_str, "%Y-%m-%d %H:%M")
    else:
        target = datetime.strptime(target_str, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
        if target <= now:
            target += timedelta(days=1)

    wait_sec = (target - now).total_seconds()
    log.info(f"예약 실행: {target.strftime('%Y-%m-%d %H:%M')} (대기 {wait_sec:.0f}초)")
    time.sleep(wait_sec)


def run_job_entry(ym_list, job_cfg, load_timeout=None):
    """단일 JOB 실행 — subprocess로 sas_to_duckdb.py 호출"""
    job_path = job_cfg["job"]
    cmd = [sys.executable, "sas_to_duckdb.py", "--ym"] + ym_list + ["--job", job_path]

    if job_cfg.get("skip_load"):
        cmd.append("--skip-load")
    if job_cfg.get("stage"):
        cmd.extend(["--stage"] + job_cfg["stage"])
    if job_cfg.get("tables"):
        cmd.extend(["--tables"] + job_cfg["tables"])

    job_timeout = job_cfg.get("timeout")
    lt = job_cfg.get("load_timeout") or load_timeout
    if lt:
        cmd.extend(["--load-timeout", str(lt)])

    log.info(f"  실행: {' '.join(cmd)}")
    t0 = time.time()

    try:
        result = subprocess.run(
            cmd,
            cwd=str(Path(__file__).parent),
            timeout=job_timeout,
        )
        elapsed = time.time() - t0
        if result.returncode == 0:
            log.info(f"  완료: {job_path} ({elapsed:.1f}초)")
        else:
            log.error(f"  실패: {job_path} (exit={result.returncode}, {elapsed:.1f}초)")
        return result.returncode
    except subprocess.TimeoutExpired:
        elapsed = time.time() - t0
        log.error(f"  타임아웃: {job_path} ({job_timeout}초 초과, {elapsed:.1f}초)")
        return -1


def main():
    setup_logger()

    parser = argparse.ArgumentParser(
        description="런셋 — 여러 JOB 연속 실행",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
예시:
  python runset.py --config runsets/daily.py
  python runset.py --config runsets/daily.py --at 06:00
  python runset.py --config runsets/daily.py --at 2026-04-04T06:00
  python runset.py --config runsets/daily.py --timeout 7200
        """
    )
    parser.add_argument("--config", "-c", required=True,
                        help="런셋 설정 파일 경로 (예: runsets/daily.py)")
    parser.add_argument("--at", dest="at_time",
                        help="예약 실행 시각 (HH:MM / 'YYYY-MM-DD HH:MM' / +30m / +2h)")
    parser.add_argument("--timeout", type=int, default=None,
                        help="전체 타임아웃(초), 런셋 설정보다 우선")
    parser.add_argument("--ym", nargs="+", default=None,
                        help="처리할 년월 (런셋 설정보다 우선)")

    args = parser.parse_args()

    # 런셋 로드
    cfg_path = Path(args.config)
    if not cfg_path.exists():
        log.error(f"런셋 파일 없음: {cfg_path}")
        sys.exit(1)

    mod = load_runset(cfg_path)
    ym_list = args.ym or mod.YM
    jobs = mod.JOBS
    timeout = args.timeout or getattr(mod, "TIMEOUT", 0)
    load_timeout = getattr(mod, "LOAD_TIMEOUT", None)

    log.info(f"런셋: {cfg_path.name}")
    log.info(f"대상 월: {', '.join(ym_list)}")
    log.info(f"JOB 수: {len(jobs)}개")
    if timeout:
        log.info(f"전체 타임아웃: {timeout}초 ({timeout//3600}시간 {(timeout%3600)//60}분)")

    # 예약 실행
    if args.at_time:
        wait_until(args.at_time)

    # 실행
    t_total = time.time()
    results = []

    for idx, job_cfg in enumerate(jobs, 1):
        # 전체 타임아웃 체크
        if timeout and (time.time() - t_total) >= timeout:
            log.error(f"전체 타임아웃 초과 ({timeout}초) — 남은 JOB 건너뜀")
            break

        job_name = job_cfg["job"]
        log.info(f"━" * 60)
        log.info(f"[{idx}/{len(jobs)}] {job_name}")
        log.info(f"━" * 60)

        # 남은 시간으로 job별 타임아웃 조정
        if timeout and not job_cfg.get("timeout"):
            remaining = timeout - (time.time() - t_total)
            if remaining > 0:
                job_cfg["timeout"] = int(remaining)

        rc = run_job_entry(ym_list, job_cfg, load_timeout=load_timeout)
        results.append((job_name, rc))

    # 결과 요약
    total_sec = time.time() - t_total
    log.info("")
    log.info("=" * 60)
    log.info(f"런셋 완료  총 소요: {total_sec:.1f}초 ({total_sec/60:.1f}분)")
    log.info("=" * 60)
    for name, rc in results:
        status = "OK" if rc == 0 else f"FAIL(exit={rc})" if rc > 0 else "TIMEOUT"
        log.info(f"  {status:12s} {name}")

    failed = [name for name, rc in results if rc != 0]
    if failed:
        log.warning(f"실패: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
