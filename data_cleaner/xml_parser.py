import re
from xml.etree import ElementTree as ET

content = ET.parse('redownload/S01012002/1200201288.xml')

# for i in content.iter():
#     print(i.tag)
#     if i.tag == 'article':
#         print({k:v for k,v in i.items()})
#     if i.tag in ['Identifica', 'Data', 'Ementa']:
#         print(i.text)
#     if i.tag == 'Texto':
#         print(i.findall('.'))
#         # print('\n'.join([x.strip() for x in re.findall(r"<p\sclass=\'corpo.+?\'>(.+?)<\/p>", i.text.replace('\n', ' ').replace('<br>', ''))]))
#     print('------------------')

# text = ET.fromstring(f"<xml> {content.findtext('.//Texto')} </xml>")

texto = re.sub(r'<\/p>\s+<p', '</p>\n<p', content.findtext('.//Texto').replace('\n', ' ').replace('<br>', ''))

print([t.strip() for t in re.findall(r"<p\sclass=\'corpo.+\'>(.+)<\/p>", texto)])
