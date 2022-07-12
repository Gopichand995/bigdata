import functools
import sys


def func1(f):
    def func2(*args):
        print('Before the f call {}({!r})'.format(f, *args))
        result = f(*args)
        print(f"After the f call: {result}")
        return result
    return func2


def memoize(f):
    cache = {}
    print('Called memoize ({!r})'.format(f))
    @functools.wraps(f)
    def temp(*args):
        print('Before the f call {}({!r})'.format(f, *args))
        if args in cache:
            print('Cache Hit!')
            return cache[args]
        if args not in cache:
            print(args)
            result = f(*args)
            cache[args] = result
            print(result)
            return result
    return temp


@memoize
def add(a, b):
    print("I am inside the add function")
    return a + b


add(int(sys.argv[1]), int(sys.argv[2]))
# if __name__ == "__main__":
#     print(add(2, 3))
