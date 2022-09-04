import os
import subprocess
import argparse


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('path', type=str, nargs=1, help='gimme the path')
args = parser.parse_args()

p = args.path[0]
for file in os.listdir(p):
    if not file.endswith(".bz2"):
        continue

    subprocess.Popen(["bzip2", "-d", f"{p}/{file}"]).wait()

