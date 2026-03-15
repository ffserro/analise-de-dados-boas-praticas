import shlex
import json
from urllib.parse import urlparse, parse_qsl, urlunparse
import requests


def parse_cookie_header(cookie_value: str) -> dict:
    cookies = {}
    for item in cookie_value.split(";"):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            k, v = item.split("=", 1)
            cookies[k.strip()] = v.strip()
    return cookies


def try_parse_data(data: str, content_type: str | None = None):
    if not data:
        return None, None

    # tenta JSON
    if content_type and "application/json" in content_type.lower():
        try:
            return "json", json.loads(data)
        except Exception:
            return "data", data

    # tenta inferir JSON mesmo sem content-type
    stripped = data.strip()
    if (stripped.startswith("{") and stripped.endswith("}")) or (
        stripped.startswith("[") and stripped.endswith("]")
    ):
        try:
            return "json", json.loads(data)
        except Exception:
            pass

    return "data", data


def curl_to_requests(curl_command: str) -> dict:
    tokens = shlex.split(curl_command)

    if not tokens or tokens[0] != "curl":
        raise ValueError("O texto fornecido não parece ser um comando cURL válido.")

    method = "GET"
    url = None
    headers = {}
    cookies = {}
    raw_data = None

    i = 1
    while i < len(tokens):
        token = tokens[i]

        if token in ("-X", "--request"):
            i += 1
            method = tokens[i].upper()

        elif token in ("-H", "--header"):
            i += 1
            header_line = tokens[i]
            if ":" in header_line:
                key, value = header_line.split(":", 1)
                key = key.strip()
                value = value.strip()

                if key.lower() == "cookie":
                    cookies.update(parse_cookie_header(value))
                else:
                    headers[key] = value

        elif token in ("--data", "--data-raw", "--data-binary", "--data-ascii", "-d"):
            i += 1
            raw_data = tokens[i]
            if method == "GET":
                method = "POST"

        elif token.startswith("http://") or token.startswith("https://"):
            url = token

        i += 1

    if not url:
        raise ValueError("Não foi possível identificar a URL no cURL.")

    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    content_type = None
    for k, v in headers.items():
        if k.lower() == "content-type":
            content_type = v
            break

    body_kind, body_value = try_parse_data(raw_data, content_type)

    result = {
        "method": method,
        "url": clean_url,
        "params": params,
        "headers": headers,
        "cookies": cookies,
    }

    if body_kind == "json":
        result["json"] = body_value
    elif body_kind == "data":
        result["data"] = body_value

    return result


def send_curl_as_request(curl_command: str, timeout: int = 60) -> requests.Response:
    req = curl_to_requests(curl_command)

    method = req.pop("method").lower()
    return requests.request(method=method, timeout=timeout, **req)


if __name__ == "__main__":
    curl_cmd = """curl 'https://in.gov.br/o/js_loader_modules?t=1767012725152' \
-X 'GET' \
-H 'Accept: */*' \
-H 'Sec-Fetch-Site: same-origin' \
-H 'Cookie: JSESSIONID=_iNvRxBmDs7t_dk5wOPzAeIN1A2hXdVIA1YjF1qx.sinvp-213; TS01ebd486=0123313e824dcb5aca3d298ef8fcb889aa365ecc151fadcf1f35ea59400f3b619d8e42c5116921036425412728b636bce320709e60e841b2155c9d3d91e30c70f754c534cc4faa0600bd4d2f9a650a8c10bee3f1b9; TS64d1d803027=088523719cab20006817b66f8750e2fa8f8ecbdfa3e2e8898183180fe59cecfe8bea081d3bd67d4108bd70d0de1130004b8611f2bfa48a03c27b7b4255f9450aa3bf4f469ae148587b80a25905ae9ae260213159c2029c25379f8c20f13e235e; _ga=GA1.3.1919416291.1772768429; _gid=GA1.3.1829040438.1773271969; LFR_SESSION_STATE_20158=1773404340654; COOKIE_SUPPORT=true' \
-H 'Referer: https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados?ano=2002&mes=Dezembro' \
-H 'Sec-Fetch-Dest: script' \
-H 'Accept-Language: pt-BR,pt;q=0.9' \
-H 'Sec-Fetch-Mode: no-cors' \
-H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15' \
-H 'Accept-Encoding: gzip, deflate, br, zstd' \
-H 'Connection: keep-alive' \
-H 'Priority: u=1, i'"""

    # req_data = curl_to_requests(curl_cmd)
    # print(req_data)
    
    import requests
    from bs4 import BeautifulSoup

    url = "https://in.gov.br/acesso-a-informacao/dados-abertos/base-de-dados"
    params = {"ano": "2002", "mes": "Dezembro"}

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    html = resp.text
    print(".zip" in html)

    soup = BeautifulSoup(html, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True) if ".zip" in a.get("href", "").lower()]

    print(f"{len(links)} links encontrados")
    print(*links[:10], sep="\n")