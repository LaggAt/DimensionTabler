
def flatten(iter):
    lst = []
    for i in iter:
        if type(i) is list or type(i) is tuple:
            lst = lst + flatten(i)
        else:
            lst = lst + [i]
    return lst