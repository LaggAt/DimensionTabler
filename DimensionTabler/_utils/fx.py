#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import __builtin__

def first(iter):
    if len(iter):
        return iter[0]
    else:
        return None

def last(iter):
    if len(iter):
        return iter[-1]
    else:
        return None

def min(iter):
    if len(iter):
        return __builtin__.min(iter)
    else:
        return None

def max(iter):
    if len(iter):
        return __builtin__.max(iter)
    else:
        return None

def avg(iter):
    if len(iter):
        return sum(iter) / count(iter)
    else:
        return None

def sum(iter):
    if len(iter):
        return __builtin__.sum(iter)
    else:
        return 0

def count(iter):
    return len(iter)