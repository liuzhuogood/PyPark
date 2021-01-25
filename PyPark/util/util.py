def unicode2str(s):
    return s.encode('utf-8').decode('unicode_escape')


def cut_list_data(data_list: list, cut_num=1) -> list:
    """
    把数组切成指定份数返回数据数组
    :param data_list: 源数组
    :param cut_num: 切份数
    :return:
    """
    # 数据的份数
    data_nums = len(data_list)
    # 数据分片开始
    cut_start = 0
    # 分片的数量
    # 分片大小,为0表示不分片
    cut_size = data_nums // cut_num
    yu = data_nums % cut_num
    results = []
    for i in range(0, cut_num):
        # 先分余
        if yu > 0:
            yu -= 1
            cut_end = cut_start + cut_size + 1
        else:
            cut_end = cut_start + cut_size
        # 是不是最后的一片
        if cut_end > data_nums - cut_size:
            cut_end = data_nums
        results.append(data_list[cut_start:cut_end])
        cut_start = cut_end
    return results


def cut_list_num(data_list: list, cut_num=1):
    """
    把数组切成指定份数返回数据下标数组
    :param data_list: 源数组
    :param cut_num: 切份数
    :return:
    """
    if data_list is None:
        return None
    # 数据的份数
    data_nums = len(data_list)
    # 数据分片开始
    cut_start = 0
    # 分片的数量
    # 分片大小,为0表示不分片
    cut_size = data_nums // cut_num
    yu = data_nums % cut_num
    results = []
    for i in range(0, cut_num):
        # 先分余
        if yu > 0:
            yu -= 1
            cut_end = cut_start + cut_size + 1
        else:
            cut_end = cut_start + cut_size
        # 是不是最后的一片
        if cut_end > data_nums - cut_size:
            cut_end = data_nums
        results.append((cut_start, cut_end))
        cut_start = cut_end
    return results


if __name__ == '__main__':
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9]

    print("把数组切成指定份数返回数据数组")
    for i in range(1, 10):
        print(f"------------切成{i}份------------")
        for r in cut_list_data(data, i):
            print(r)

    print("把数组切成指定份数返回数据下标数组")
    for i in range(1, 10):
        print(f"------------切成{i}份------------")
        for r in cut_list_num(data, i):
            s, e = r
            print(s, e, "---->", data[s:e])
