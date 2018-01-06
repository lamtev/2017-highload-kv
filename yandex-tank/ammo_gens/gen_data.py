#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys


def main():
    if len(sys.argv) != 5:
        sys.stdout.write('Invalid number of arguments')
        return

    mthd = sys.argv[1]
    rf = sys.argv[2]
    n = sys.argv[3]
    ow = sys.argv[4] == '+'
    rfstr = rf.replace('/', '')
    owstr = '_ow' if ow else ''
    file = open('data_{mthd}_{rf}_{n}{ow}'.format(mthd=mthd, rf=rfstr, n=n, ow=owstr), 'w')

    if mthd == 'PUTGET':
        N = int(int(n) / 2)
        ow_n_times = 2 if ow else 1
        N = int(N / 2) if ow else N
        for i in range(0, N):
            ID = i
            for j in range(0, ow_n_times):
                mthd = 'PUT'
                tag = '{mthd}_{rf}'.format(mthd=mthd, rf=rf)
                body = '||hello!! yandex-tank load test!!! gfsdgwergiwj4ofrgjoi4jgedfghweiorh'
                file.write('{m}||/v0/entity?id={id}&replicas={rf}||{tag}{body}\n'.format(m=mthd, id=ID, rf=rf, tag=tag, body=body))
                mthd = 'GET'
                tag = '{mthd}_{rf}'.format(mthd=mthd, rf=rf)
                body = ''
                file.write('{m}||/v0/entity?id={id}&replicas={rf}||{tag}{body}\n'.format(m=mthd, id=ID, rf=rf, tag=tag, body=body))
        return

    tag = '{mthd}_{rf}'.format(mthd=mthd, rf=rf)
    N = int(n)
    ow_n_times = 2 if ow else 1
    N = int(N / 2) if ow else N
    for i in range(0, N):
        ID = i
        for j in range(0, ow_n_times):
            if mthd == 'PUT':
                body = '||hello!! yandex-tank load test!!! gfsdgwergiwj4ofrgjoi4jgedfghweiorh'
            else:
                body = ''
            file.write('{m}||/v0/entity?id={id}&replicas={rf}||{tag}{body}\n'.format(m=mthd, id=ID, rf=rf, tag=tag, body=body))


if __name__ == "__main__":
    main()
