#!/usr/bin/env python
"""build."""

import concurrent.futures
import pathlib
import subprocess


def build(path):
    """build."""
    stem = path.stem
    opath = (
        path
        .with_suffix('')
        .with_name(stem + '_spec')
        .with_suffix('.lua'))
    with opath.open('w') as ostream:
        subprocess.Popen(
            ['python3', '-m', 'yaml_to_lua.py', str(path.resolve())],
            cwd='spec',
            stdout=ostream).wait(20)


def main():
    """main."""
    fs = set()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for path in pathlib.Path().rglob('*.yaml'):
            if '.rb.' in str(path):
                continue
            fs.add(executor.submit(build, path))
        concurrent.futures.wait(fs, timeout=600)

if __name__ == '__main__':
    main()
