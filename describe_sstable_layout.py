#!/usr/bin/env python3

import argparse
import sstable_tools.scylla
import sstable_tools.statistics
import os
import sys
from collections import defaultdict

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

class sstable:
    filename = ""
    size = 0
    live_count = 0
    dead_count = 0

    def __init__(self, f, s, l, d):
        self.filename = f
        self.size = s
        self.live_count = l
        self.dead_count = d
        
    def describe(self):
        return "{ %s: size: %s, live: %d, dead: %d }" % (self.filename, sizeof_fmt(self.size), self.live_count, self.dead_count)
    
def get_run_size(run):
    run_size = 0
    for sst in run[1]:
        run_size += sst.size
    return run_size

def get_run_id(scylla_file, sst_format):
    with open(scylla_file, 'rb') as f:
        data = f.read()

    metadata = sstable_tools.scylla.parse(data, sst_format)
    run_id_struct = metadata['data']['run_identifier']['id']
    return str(run_id_struct['msb']) + str(run_id_struct['lsb'])

def get_live_and_dead_count(stats_file, sst_format):
    with open(stats_file, 'rb') as f:
        data = f.read()

    metadata = sstable_tools.statistics.parse(data, sst_format)
    
    live_count = metadata['Stats']['rows_count']
    dead_count = 0
    elements = metadata['Stats']['estimated_tombstone_drop_time']['elements']
    for element in elements:
        dead_count += element['value']
    
    return live_count,dead_count

class per_shard_info:
    shard_id = 0
    runs_to_sstables = []
    
    def __init__(self, shard_id):
        self.shard_id = shard_id
        self.runs_to_sstables = defaultdict(set)

    def add_sstable(self, run_id, sst):
        self.runs_to_sstables[run_id].add(sst)

    def dump(self):
        print("--- SHARD #{} ---".format(self.shard_id))
        for run_id,sstables in sorted(self.runs_to_sstables.items(), key=get_run_size, reverse=True):
            run_size = 0
            run_live_count = 0
            run_dead_count = 0
            sst_descriptions = ""
            for sst in sstables:
                run_size += sst.size
                run_live_count += sst.live_count
                run_dead_count += sst.dead_count
                sst_descriptions += "\n\t" + sst.describe()
            print("[Run %s: size: %s, live: %d, dead: %d %s\n]" % (run_id, sizeof_fmt(run_size), run_live_count, run_dead_count, sst_descriptions))

def main():
    if len(sys.argv) != 3:
        print("usage: {} /path/to/table shards".format(sys.argv[0]))
        exit(1)

    directory=sys.argv[1]
    shards=sys.argv[2]
    
    per_shard_info_set = dict()
    for shard_id in range(0, int(shards)):
        per_shard_info_set[shard_id] = per_shard_info(shard_id)

    for filename in os.listdir(directory):
        if not filename.endswith("Scylla.db"):
            continue
        sst_format = ""
        if filename.startswith("mc-"):
            sst_format = "mc"
        elif filename.startswith("md-"):
            sst_format = "md"
        else:
            print("unable to find sst format in {}", filename)
            exit(1)

        scylla_file = os.path.join(directory, filename)
        run_id = get_run_id(scylla_file, sst_format)

        data_file = scylla_file.replace("Scylla.db", "Data.db")
        size = os.stat(data_file).st_size
        
        stats_file = scylla_file.replace("Scylla.db", "Statistics.db")
        live_count, dead_count = get_live_and_dead_count(stats_file, sst_format)
        
        sst = sstable(filename, size, live_count, dead_count)
        sst_generation = ([int(s) for s in filename.split('-') if s.isdigit()])[0]
        shard_id = sst_generation % int(shards)
        per_shard_info_set[shard_id].add_sstable(run_id, sst)

    for shard_id in range(0, int(shards)):
        per_shard_info_set[shard_id].dump()

main()
