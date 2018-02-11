#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

def flatten(iter):
    lst = []
    for i in iter:
        if type(i) is list or type(i) is tuple:
            lst = lst + flatten(i)
        else:
            lst = lst + [i]
    return lst