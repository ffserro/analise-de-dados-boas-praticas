import re
from xml.etree import ElementTree as ET

content = ET.parse('test_file/1200701036.xml')

for i in content.iter():
    print(i.tag)
    if i.tag == 'article':
        print({k:v for k,v in i.items()})
    if i.tag in ['Identifica', 'Data', 'Ementa']:
        print(i.text)
    if i.tag == 'Texto':
        print('\n'.join([x.strip() for x in re.findall(r"<p\sclass=\'corpo.+?\'>(.+?)<\/p>", i.text.replace('\n', ' ').replace('<br>', ''))]))
    print('------------------')