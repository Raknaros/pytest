def test_func(a):
    b = a + 5
    return b


def test_func2(c):
    d = c + 10
    return d


def test_func3():
    f = 25

    return print(test_func2(test_func(f)))

test_func3()

