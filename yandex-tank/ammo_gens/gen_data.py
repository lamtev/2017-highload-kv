#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys


def main():
    if len(sys.argv) != 5:
        sys.stdout.write('Invalid number of arguments')
        return

    method: str = sys.argv[1].upper()
    rf: str = sys.argv[2]
    n: str = sys.argv[3]
    ow: bool = sys.argv[4] == '+'

    rfstr = rf.replace('/', '')
    owstr = '_ow' if ow else ''
    file = open('data_{method}_{rf}_{n}{ow}'.format(method=method, rf=rfstr, n=n, ow=owstr), 'w')

    if method == 'PUTGET':
        N = int(int(n) / 2)
        ow_n_times = 2 if ow else 1
        N = int(N / 2) if ow else N
        for i in range(0, N):
            ID = i
            for j in range(0, ow_n_times):
                method = 'PUT'
                body = '||hello!! yandex-tank load test!!! gfsdgwergiwj4ofrgjoi4jgedfghweiorh'
                file.write('{m}||/v0/entity?id={id}&replicas={rf}||tag{body}\n'.format(m=method, id=ID, rf=rf, body=body))
                method = 'GET'
                body = ''
                file.write('{m}||/v0/entity?id={id}&replicas={rf}||tag{body}\n'.format(m=method, id=ID, rf=rf, body=body))
        return

    N = int(n)
    ow_n_times = 2 if ow else 1
    N = int(N / 2) if ow else N
    for i in range(0, N):
        ID = i
        for j in range(0, ow_n_times):
            if method == 'PUT':
                body = '||hello!! yandex-tank load test!!! gfsdgwergiwj4ofrgjoi4jgedfghweiorh'
            else:
                body = ''
            file.write('{m}||/v0/entity?id={id}&replicas={rf}||tag{body}\n'.format(m=method, id=ID, rf=rf, body=body))


if __name__ == "__main__":
    main()