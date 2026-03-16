"""
Oracle 접속 정보 설정

job 파일에서 import하여 사용:
    from oracle_config import ORA_DEV, ORA_PROD

TABLES 정의 예시:
    "ora_table": {
        "type": "oracle",
        **ORA_DEV,
        "sql": "SELECT * FROM schema.table WHERE yyyymm = '{yyyymm}'",
        "month_col": None,
    },
"""

# 개발 DB
ORA_DEV = {
    "dsn": "dev-host:1521/dev_service",
    "user": "dev_user",
    "password": "dev_pass",
}

# 운영 DB
ORA_PROD = {
    "dsn": "prod-host:1521/prod_service",
    "user": "prod_user",
    "password": "prod_pass",
}
