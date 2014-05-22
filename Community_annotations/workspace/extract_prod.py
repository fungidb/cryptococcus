#!/usr/bin/env python3

import os
import glob
import csv
import sys
from collections import namedtuple


MD = "/eupath/data/EuPathDB/manualDelivery/FungiDB"

ORGS = ("CneoH99", "CneoJEC21", "CneoB-3501A", "CgatR265", "CgatWM276")

PRODNAMES = "function/Community_product_names"
GENENAMES = "function/Community_gene_names"
ALIASES = "alias/Community_gene_names"
DBXREFS = "dbxref/Community_gene_names"
VERSION = "2014-04-30"

NEGATIVES = ("", "NONE", "NOT CALLED", "NONE CALLED", "NONE?", "SPLIT ORF", "UNSURE")

CSVFILE = os.path.join(MD, "cryptococcus",
                       "Community_annotations", "fromProvider",
                       "Crypto_supplement.csv")

COLS = ["CneoH99 ID", "CneoH99 Standard name", "CneoJEC21 ID",
        "CneoJEC21 Standard name", "CneoB-3501A ID",
        "CneoB-3501A Standard name", "CgatWM276 ID", "CgatWM276 Standard name",
        "CgatR265 ID", "CgatR265 Standard name", "New description"]

def extract_csv(file, keys):
    header = next(file)
    idx = [header.index(k) for k in keys]
    rows = {k: [] for k in keys}
    for row in file:
        for k, i in zip(keys, idx):
            rows[k].append(row[i])
    return rows

def split_alias(tup):
    k, values = tup
    v = []
    a = []
    for name in values.split():
        if name.startswith('('):
            a.append(name.lstrip('(').rstrip(')'))
        else:
            v.append(name)
    return k, v, a

def parse_prod(file):
    return {k: v for k, v in (line.rstrip().split('\t') for line in file)}

def merge(file, prods):
    mprods = parse_prod(file)
    mprods.update(prods)
    return sorted(mprods.items())

def export(values):
    return '\n'.join(map('\t'.join, values))

def rename_old():
    for d in (PRODNAMES, GENENAMES, ALIASES, DBXREFS):
        for o in ORGS:
            path = os.path.join(MD, o, d, VERSION, "final")
            for f in glob.glob(path+"/*.txt"):
                os.rename(f, f+".old")

def filter_prodnames(keys, values):
    return sorted((k, v) for k, v in zip(keys, values)
                  if v and k.upper() not in NEGATIVES)

def process_prodnames(rows):
    for o in ORGS:
        mpath = os.path.join(MD, o, "function")
        mfile = next(glob.glob(os.path.join(mpath, d) + "/*/final/*.txt")
                     for d in os.listdir(mpath)
                     if not d.startswith("Community")
                     and d.endswith("product_names"))
        ofile = os.path.join(MD, o, PRODNAMES, VERSION, "final", "products.txt")
        key = o + " ID"
        val = "New description"
        prods = filter_prodnames(rows[key], rows[val])
        if mfile:
            with open(mfile[0]) as infile:
                prods = merge(infile, prods)
        with open(ofile, 'w') as outfile:
            outfile.write(export(prods))

def filter_genenames(keys, values):
    return sorted((k, v[0]) for k, v, a in map(split_alias, zip(keys, values))
                  if v and k.upper() not in NEGATIVES)

def process_genenames(rows):
    for o in ORGS:
        ofile = os.path.join(MD, o, GENENAMES, VERSION, "final", "geneName.txt")
        key = o + " ID"
        val = o + " Standard name"
        prods = filter_genenames(rows[key], rows[val])
        with open(ofile, 'w') as outfile:
            outfile.write(export(prods))

def filter_aliases(keys, values):
    return sorted((k, v + a) for k, v, a in map(split_alias, zip(keys, values))
                  if (v or a) and k.upper() not in NEGATIVES)

def flatten(values):
    for k, val in values:
        for v in val:
            yield (k, v)

def process_aliases(rows):
    for o in ORGS:
        ofile = os.path.join(MD, o, ALIASES, VERSION, "final", "aliases.txt")
        key = o + " ID"
        val = o + " Standard name"
        aliases = filter_aliases(rows[key], rows[val])
        with open(ofile, 'w') as outfile:
            outfile.write(export(flatten(aliases)))

def process_dbxrefs(rows):
    for o in ORGS:
        ofile = os.path.join(MD, o, DBXREFS, VERSION, "final", "mapping.txt")
        key = o + " ID"
        val = o + " Standard name"
        aliases = filter_aliases(rows[key], rows[val])
        with open(ofile, 'w') as outfile:
            outfile.write(export(flatten(aliases)))

def main():
    with open(CSVFILE) as infile:
        rows = extract_csv(csv.reader(infile), COLS)
    process_prodnames(rows)
    process_genenames(rows)
    process_aliases(rows)
    process_dbxrefs(rows)


if __name__ == "__main__":
    main()
