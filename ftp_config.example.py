"""
FTP 접속 정보 설정

job 파일에서 import하여 사용:
    from ftp_config import FTP_IFRS4

prejob 예시:
    from ftp_config import FTP_IFRS4

    def prejob(yyyymm):
        download_ftp(FTP_IFRS4, yyyymm)
"""

# IFRS4 FTP
FTP_IFRS4 = {
    "host": "10.150.3.200",
    "port": 2047,
    "user": "D030060",
    "password": "비밀번호",
    "remote_dir": "/IFRS4/",
    "encoding": "cp949",        # FTP 서버 인코딩 (한글 파일명)
}
