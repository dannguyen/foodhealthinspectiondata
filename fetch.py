from pathlib import Path
from urllib.parse import urlparse, urlunparse
import requests
import csv
import re
from zipfile import ZipFile
LIST_URL = 'https://docs.google.com/spreadsheets/d/1t1-gLpFMyqFLqrQtY-QR6-QK5DcbSAJS7dc15L6UNAI/export?format=csv&id=1t1-gLpFMyqFLqrQtY-QR6-QK5DcbSAJS7dc15L6UNAI&gid=1367762216'
DATA_DIR = Path('data')

for d in csv.DictReader(requests.get(LIST_URL).text.splitlines()):
    if d['zip_url']:
        url = d['zip_url']
        bname = Path(url).name
    elif d['socrata_url']:
        u = urlparse(d['socrata_url'])
        slug = re.search(r'(?<=/)[a-z0-9]{4}-[a-z0-9]{4}\b', u.path).group()
        up = 'api/views/{0}/rows.csv?accessType=DOWNLOAD'.format(slug)
        bname = slug + '.csv'
        url = urlunparse((u.scheme, u.netloc, up, '', '', ''))
    else:
        continue
    xdir = DATA_DIR.joinpath(d['country'].lower(), d['state'].lower())
    if d.get('county'):
        xdir = xdir.joinpath('counties', d.get('county').lower().replace(' ', '_'))
    elif d.get('city'):
        xdir = xdir.joinpath('cities', d.get('city').lower().replace(' ', '_'))
    elif d.get('district'):
        xdir = xdir.joinpath('districts', d.get('district').lower().replace(' ', '_'))
    xdir.joinpath('raw').mkdir(parents=True, exist_ok=True)
    xpath = xdir.joinpath('raw', bname)
    if xpath.exists():
        print(xpath, "already exists")
        sdir = xdir.joinpath('samples')
        sdir.mkdir(parents=True, exist_ok=True)
        if 'csv' in xpath.name:
            sdir.joinpath(bname).write_text("\n".join(xpath.read_text().splitlines()[0:1000]))
        else: # zipfile
            zfile = ZipFile(xpath.open('rb'))
            for n in zfile.namelist():
                zfile.extract(n, path=str(xdir))
                ypath = sdir.joinpath(n)
                print(ypath)
                with xdir.joinpath(n).open('r', encoding='latin1') as rf:
                    ypath.write_text('\n'.join(rf.readlines()[0:1000]))

    else:
        print("Downloading", url, "\n to", xpath)
        resp = requests.get(url)
        if 'csv' in xpath.name:
            xpath.write_text(resp.text)
        else: # zip file
            xpath.write_bytes(resp.content)

