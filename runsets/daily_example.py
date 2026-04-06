"""
런셋 예시: 일일 배치 (job1 → job2 → job3 순차 실행)

실행:
  python runset.py --config runsets/daily_example.py
  python runset.py --config runsets/daily_example.py --at 06:00
  python runset.py --config runsets/daily_example.py --ym 202603
"""

# 처리 대상 년월
YM = ["202603"]

# DB 파일명 (${SDM}, ${LM} 등 치환 가능)
DB_NAME = "${SDM}.duckdb"               # → db/202603.duckdb
# DB_NAME = "ifrs4-expense.duckdb"      # 단일 DB 사용 시

# 외부 DB attach (READ_ONLY). SQL에서 LM.테이블명 으로 접근 가능
ATTACH = {
    "LM": "${LM}.duckdb",              # → db/202602.duckdb AS LM
    # "LM2": "${LM2}.duckdb",          # 2개월 전
}
# ATTACH = {}                           # attach 안 함

# 전체 타임아웃 (초), 0=무제한
TIMEOUT = 14400  # 4시간

# 테이블당 읽기 타임아웃 (초), 전체 JOB에 적용
LOAD_TIMEOUT = 3600  # 1시간

# JOB 목록 (순서대로 실행)
JOBS = [
    {
        "job": "jobs/job1.py",
        # "timeout": 7200,              # 이 JOB만 2시간 제한 (선택)
        # "skip_load": True,            # LOAD 생략 (선택)
        # "stage": ["load"],            # 특정 단계만 (선택)
        # "tables": ["fio841"],         # 특정 테이블만 (선택)
        # "load_timeout": 1800,         # 이 JOB만 테이블당 30분 (선택)
        # "force_load": True,           # 강제 재로드 (선택)
        # "attach": {"LM": "other.duckdb"},  # 이 JOB만 별도 attach (선택)
    },
    {
        "job": "jobs/job2.py",
    },
    {
        "job": "jobs/job3.py",
    },
]
