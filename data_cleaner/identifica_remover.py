import re
from glob import glob


for file in glob('redownload/**/*.xml'):
    with open(file, 'rb') as f:
        content = f.read()
        content = (re.sub(r'</Identifica>"', r'"', content.decode('utf-8')))

    with open(file, 'wb') as f:
        f.write(content.encode('utf-8'))