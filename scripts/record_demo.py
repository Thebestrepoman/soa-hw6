#!/usr/bin/env python3
"""Записывает живую демонстрацию в текст и asciicast v2."""
import json
import subprocess
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parent.parent
artifacts = root / 'artifacts'
artifacts.mkdir(exist_ok=True)
start = time.monotonic()
with (artifacts / 'demo.cast').open('w') as cast, (artifacts / 'demo-full.txt').open('w') as transcript:
    cast.write(json.dumps({'version': 2, 'width': 132, 'height': 36,
                           'timestamp': int(time.time()), 'title': 'Smart Warehouse — демонстрация',
                           'env': {'TERM': 'xterm-256color', 'SHELL': '/bin/bash'}}) + '\n')
    process = subprocess.Popen([sys.executable, '-u', str(root / 'scripts/demo.py'), *sys.argv[1:]],
                               cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in process.stdout:
        print(line, end='', flush=True)
        transcript.write(line)
        transcript.flush()
        cast.write(json.dumps([round(time.monotonic() - start, 3), 'o', line.replace('\n', '\r\n')],
                              ensure_ascii=False) + '\n')
        cast.flush()
    sys.exit(process.wait())
