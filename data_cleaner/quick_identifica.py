from glob import glob
from xml.etree import ElementTree as ET

for file in glob('redownload/**/*.xml'):
    try:
        content = ET.parse(file)
        for i in content.iter():
            if i.tag == 'article':
                print(file, i.get('artType'), sep=': ')
    except:
        print(f"Erro ao processar {file}")
        break