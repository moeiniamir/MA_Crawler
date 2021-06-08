import http.client
import time
from tqdm import tqdm
import sys
import json
import argparse
from slugify import slugify
import traceback

parser = argparse.ArgumentParser()
parser.add_argument("crawl_count", help="number of articles to crawl", type=int, default=50, nargs='?')
args = parser.parse_args()
crawl_count = args.crawl_count

DATA_PATH = './data/'
EXCEPTION_PATH = './exc/'
DELAY_PER_REQUEST = 1.5
CONN_LIFETIME = 30
EXCEPTION_LIMIT = 10

# contains url_ids
queue = []
# contains best_id & id
seen = set()
# contains all articles extracted
data = []


class Article:
    def __init__(self, pfid, title, abstract, date, authors, references):
        self.id = pfid
        self.title = title
        self.abstract = abstract
        self.date = date
        self.authors = authors
        self.references = references

    def to_json(self):
        return json.dumps(self.__dict__)

    def save(self):
        with open(DATA_PATH + slugify(str(self.id)) + '.json', 'w') as file:
            file.write(self.to_json())


def initialize_queue():
    with open('start.txt') as s:
        for line in s:
            url_id = int(line.split('/')[-1])
            queue.append(url_id)


def consume_json(j):
    j = j['entity']
    id = j['id']
    best_id = j.get('pfi', j['v'].get('doi', id))

    # never happens (except for the third initial article) because all references are with id to parent paper and
    # that's checked before fetch.
    if best_id in seen:
        seen.add(id)
        return False

    title = j['dn']
    abstract = j['d']
    pub_year = int(j['v']['publishedYear'])
    authors = []
    for author in j['a']:
        authors.append(author['dn'])
    references = j['r'][:10]

    # queue references for future crawl
    queue.extend(references)
    # save this article
    article = Article(id, title, abstract, pub_year, authors, references)
    data.append(article)
    article.save()
    # saw this article
    seen.add(id)
    seen.add(best_id)
    return True


def crawl():
    conn = http.client.HTTPSConnection("academic.microsoft.com")
    conn_t = time.time()
    extract_counter = 0
    exc_no = 0
    t = 0
    with tqdm(total=crawl_count, file=sys.stdout) as pbar:
        try:
            while extract_counter < crawl_count:
                if time.time() - conn_t > CONN_LIFETIME:
                    conn.close()
                    conn = http.client.HTTPSConnection("academic.microsoft.com")
                    conn_t = time.time()
                next_id = queue.pop()
                if next_id not in seen:
                    # tqdm.write('\n' + str(next_id))
                    t_elapsed = time.time() - t
                    remaining_time = DELAY_PER_REQUEST - t_elapsed
                    if remaining_time > 0:
                        time.sleep(remaining_time)
                    conn.request("GET", f'/api/entity/{next_id}?entityType=2')
                    t = time.time()

                    resp = conn.getresponse()
                    body = resp.read()
                    tqdm.write('\n' + str(resp.status))
                    if resp.status == 200:
                        body_str = body.decode('utf-8')
                        j = json.loads(body_str)
                        try:
                            next_id = -1
                            not_seen2 = consume_json(j)
                        except KeyError as ke:
                            with open(f'{EXCEPTION_PATH}errored{exc_no}.json', 'w') as f:
                                json.dump(j, f)
                            raise ke

                        if not_seen2:
                            extract_counter += 1
                            pbar.update(1)
                    else:
                        queue.insert(0, next_id)
        except Exception as e:
            if next_id != -1:
                queue.insert(0, next_id)

            tb = str(traceback.format_exc())
            with open(f'{EXCEPTION_PATH}exception{exc_no}.txt', 'w') as f:
                f.write(tb)
            exc_no += 1

            if exc_no >= EXCEPTION_LIMIT:
                tqdm.write(f'too many exceptions({EXCEPTION_LIMIT}), terminating...')
                conn.close()
                exit(1)
    conn.close()


initialize_queue()
crawl()

json_list = []
for article in data:
    json_list.append(article.__dict__)
with open('all_data.json', 'w') as f:
    json.dump(json_list, f)
