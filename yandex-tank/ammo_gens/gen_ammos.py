#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

methods = ['PUT', 'GET', 'PUTGET']
rfs = ['2/3', '3/3']
ns = ['70000']
ows = ['-', '+']
directory = '../ammos'

if not os.path.exists(directory):
    os.makedirs(directory)
os.chdir(directory)

for mthd in methods:
    for rf in rfs:
        for n in ns:
            for ow in ows:
                os.system('python3 ../ammo_gens/gen_data.py {m} {rf} {n} {ow}'.format(m=mthd, rf=rf, n=n, ow=ow))

for f in os.listdir('.'):
    newf = f.replace('data', 'ammo')
    os.system('cat {f} | python3 ../ammo_gens/make_ammo.py > {newf}.txt'.format(f=f, newf=newf))
