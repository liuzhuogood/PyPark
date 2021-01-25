def path_join(*aa: str):
    path = []
    for a in aa:
        if a.startswith("/"):
            a = a[1:]
        path.append(a)
    return "/".join(path)
