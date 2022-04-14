import argparse
import glob
import gzip

pattern = '?????'


def new_docno(file_number, line_number):
    return f'en.noclean.c4-train.{file_number}-of-07168.{line_number}'

path='/data/c4/'
files = sorted(list(glob.iglob(f'{path}/en/c4-train.{pattern}-of-01024.json.gz')))

for filepath in files:
    with gzip.open(filepath) as f:
        file_number = filepath[-22:-22 + 5]
        file_name = filepath[-31:]
        print(f"adding docnos to file number {file_number} ...")
        with gzip.open(f'{path}/en.withdocnos/{file_name}', 'wb') as o:
            for line_number, line in enumerate(f.readlines()):
                line = line.decode('utf-8')
                new_line = f"{{\"docno\":\"{new_docno(file_number, line_number)}\",{line[1:]}"
                o.write(new_line.encode('utf-8'))

