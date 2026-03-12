import re
from playwright.sync_api import sync_playwright

# import urllib.request

# try:
#     with urllib.request.urlopen("https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados?ano=2026&mes=Janeiro") as response:
#         html = response.read().decode('utf-8')

#         for tag_a in re.findall(r'<a.+</a>', html):
#             print(re.search(r'href=\"(http\:\/\/www\.in\.gov\.br.+)\"', tag_a).group(1))

# except urllib.error.URLError as e:
#     print(f'Error: {e.reason}')

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    for ano in range(2002, 2027):
        for mes in ['Janeiro', 'Fevereiro', 'Marco', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']:
            page.goto(f"https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados?ano={ano}&mes={mes}")
            links = page.locator('a').all()
            links = [link.get_attribute('href') for link in links]
            links = [link for link in links if link and '.zip' in link]
            with open('/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/documentos/dou_zips.txt', 'a+', encoding='utf-8') as file:
                for link in links:
                    file.write(link+'\n')
            print(*links, sep="\n")
            
