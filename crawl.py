import subprocess

# URL của trang web cần tải
url = 'https://ex-coders.com/html/cluehost/news-grid.html'

# Lệnh wget với các tùy chọn cần thiết
command = [
    'wget',
    '--mirror',
    '--convert-links',
    '--adjust-extension',
    '--page-requisites',
    '--no-parent',
    url
]

# Chạy lệnh wget
subprocess.run(command)
