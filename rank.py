import networkx as nx
import argparse
from pathlib import Path
import json

DATA_PATH = './data/'

parser = argparse.ArgumentParser()
parser.add_argument("alpha", help="teleportation probability", type=float, default=.85, nargs='?')
args = parser.parse_args()
alpha = args.alpha

G = nx.DiGraph()

for file in Path(DATA_PATH).iterdir():
    with open(file) as f:
        j = json.load(f)
        if "neural probabilistic" in j['title']:
            print(file.stem)
        G.add_edges_from([(int(file.stem), int(to)) for to in j['references']])

rank = nx.pagerank(G, alpha, max_iter=1000000, tol=1e-20)
sorted_rank = sorted(rank.items(), key=lambda p: -p[1])

# print(sorted(G.in_degree, key=lambda p: -p[1]))

for pos in sorted_rank[:10]:
    p = Path(DATA_PATH).joinpath(str(pos[0]) + '.json')
    title = 'not crawled'
    if p.exists():
        with open(p) as f:
            title = json.load(f)['title']
    print('id:', pos[0], 'score:', '{:.7f}'.format(pos[1]), 'title:', title)
