"""
런셋(RunSet) — 여러 JOB을 연속 실행 + 전체 타임아웃 + 예약 실행

sas_to_duckdb.py를 직접 import하여 같은 프로세스에서 실행.
Ctrl+C, FTP 등 모든 기능이 정상 동작.

사용법:
  python runset.py --config runsets/daily.py
  python runset.py -c runsets/daily.py --at 06:00
  python runset.py -c runsets/daily.py --at "2026-04-04 06:00"
  python runset.py -c runsets/daily.py --at +30m
  python runset.py -c runsets/daily.py --at +2h
  python runset.py -c runsets/daily.py --timeout 7200

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

print("[runset] 시작")

import sys
import time
import logging
import argparse
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta

log = logging.getLogger("pipeline")


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


def main():
    # sas_to_duckdb.py는 여기서 import (모듈 로드 시 duckdb/pandas 등 무거운 import 방지)
    print("[runset] sas_to_duckdb 로딩 중...")
    from sas_to_duckdb import (
        load_job_module, run_job, setup_logger,
        _shutdown, _get_total_ram, ROOT,
    )
    import duckdb
    print("[runset] 로딩 완료")

    global log
    log = setup_logger()

    print("[runset] argparse 시작")
    parser = argparse.ArgumentParser(
        description="런셋 — 여러 JOB 연속 실행",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
예시:
  python runset.py --config runsets/daily.py
  python runset.py --config runsets/daily.py --at 06:00
  python runset.py --config runsets/daily.py --at "2026-04-04 06:00"
  python runset.py --config runsets/daily.py --at +30m
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
    db_name_tpl = getattr(mod, "DB_NAME", "${SDM}.duckdb")
    attach_tpl = getattr(mod, "ATTACH", {})

    log.info(f"런셋: {cfg_path.name}")
    log.info(f"대상 월: {', '.join(ym_list)}")
    log.info(f"JOB 수: {len(jobs)}개")
    if timeout:
        log.info(f"전체 타임아웃: {timeout}초 ({timeout//3600}시간 {(timeout%3600)//60}분)")
    if load_timeout:
        log.info(f"테이블당 타임아웃: {load_timeout}초")

    # 실행 계획 출력
    log.info("")
    log.info("─" * 60)
    log.info("실행 계획:")
    log.info("─" * 60)
    for idx, job_cfg in enumerate(jobs, 1):
        job_name = job_cfg["job"]
        opts = []
        if job_cfg.get("stage"):
            opts.append(f"stage={job_cfg['stage']}")
        if job_cfg.get("tables"):
            resolved = [t.replace("{yyyymm}", ym_list[0]) for t in job_cfg["tables"]]
            opts.append(f"tables={resolved}")
        if job_cfg.get("skip_load"):
            opts.append("skip_load")
        if job_cfg.get("timeout"):
            opts.append(f"timeout={job_cfg['timeout']}초")
        opt_str = f"  ({', '.join(opts)})" if opts else ""
        log.info(f"  [{idx}] {job_name}{opt_str}")
    log.info("─" * 60)
    log.info("")

    # 예약 실행
    if args.at_time:
        wait_until(args.at_time)

    # JOB 모듈 사전 로드
    job_mods = []
    for job_cfg in jobs:
        try:
            job_mods.append((job_cfg, load_job_module(job_cfg["job"])))
        except (FileNotFoundError, AttributeError) as e:
            log.error(str(e))
            sys.exit(1)

    # 실행: 월별로 DB 파일 분리 (YYYYMM.duckdb)
    t_total = time.time()
    results = []

    def _open_con(yyyymm):
        """DuckDB 연결 생성 (DB_NAME 템플릿 기반)"""
        from sas_to_duckdb import build_params, _replace_params
        params = build_params(yyyymm)
        db_file = _replace_params(db_name_tpl, params)
        db_path = ROOT / "db" / db_file
        db_path.parent.mkdir(parents=True, exist_ok=True)
        log.info(f"DB: {db_path}")
        c = duckdb.connect(str(db_path))
        total_ram = _get_total_ram()
        if total_ram:
            mem_limit = max(int(total_ram * 0.75) // (1024 ** 3), 4)
            c.execute(f"SET memory_limit = '{mem_limit}GB'")
            log.info(f"DuckDB memory_limit = {mem_limit}GB (시스템 RAM {total_ram // (1024**3)}GB의 75%)")
        else:
            c.execute("SET memory_limit = '4GB'")
        c.execute("SET temp_directory = 'duckdb_tmp'")
        c.execute("SET preserve_insertion_order = false")
        try:
            c.execute("LOAD encodings")
            log.info("DuckDB encodings 확장 로드 (cp949 직접 지원)")
        except Exception:
            pass
        return c

    for yyyymm in ym_list:
        if _shutdown.is_set():
            log.warning("종료 요청으로 남은 월 건너뜀")
            break

        con = _open_con(yyyymm)
        try:
            for idx, (job_cfg, job_mod) in enumerate(job_mods, 1):
                if _shutdown.is_set():
                    log.warning("종료 요청으로 남은 JOB 건너뜀")
                    break

                # 전체 타임아웃 체크
                if timeout and (time.time() - t_total) >= timeout:
                    log.error(f"전체 타임아웃 초과 ({timeout}초) — 남은 JOB 건너뜀")
                    break

                job_name = job_cfg["job"]
                log.info(f"━" * 60)
                log.info(f"[{idx}/{len(job_mods)}] {job_name}  (월: {yyyymm})")
                log.info(f"━" * 60)

                try:
                    # attach 설정: 런셋 전역 + JOB별 오버라이드
                    job_attach = {**attach_tpl, **job_cfg.get("attach", {})}
                    run_job(
                        con, job_mod, yyyymm,
                        skip_load=job_cfg.get("skip_load", False),
                        stages=job_cfg.get("stage"),
                        only_tables=job_cfg.get("tables"),
                        load_timeout=job_cfg.get("load_timeout") or load_timeout,
                        force_load=job_cfg.get("force_load", False),
                        attach_dbs=job_attach or None,
                    )
                    results.append((f"{yyyymm}/{job_name}", 0))
                except Exception as e:
                    log.error(f"[{job_name}] 실패: {e}")
                    results.append((f"{yyyymm}/{job_name}", 1))
        finally:
            con.close()
            log.info(f"DB 닫음: {yyyymm}.duckdb")

    # 결과 요약
    total_sec = time.time() - t_total
    log.info("")
    log.info("=" * 60)
    log.info(f"런셋 완료  총 소요: {total_sec:.1f}초 ({total_sec/60:.1f}분)")
    log.info("=" * 60)
    for name, rc in results:
        status = "OK" if rc == 0 else "FAIL"
        log.info(f"  {status:6s} {name}")

    failed = [name for name, rc in results if rc != 0]
    if failed:
        log.warning(f"실패: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
