def find_sum(n):
    if n == 1:
        return 1
    return n + find_sum(n - 1)


def fib(n):
    if n == 0 or n == 1:
        return n
    return fib(n - 1) + fib(n - 2)


def lucas(n):
    if n == 0:
        return 2
    elif n == 1:
        return 1
    else:
        return lucas(n - 1) + lucas(n - 2)


if __name__ == "__main__":
    # print(find_sum(5))
    test = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    for i in test:
        print(fib(i))


# from collections import namedtuple
# N = int(input())
# fields = input().split()
# sum = 0
# for _ in range(N):
#     Student = namedtuple('Student', fields)
#     field1, field2, field3, field4 = input().split()
#     student = Student(field1, field2, field3, field4)
#     sum += int(student.MARKS)
# print('{:.2f}'.format(sum/N, 2))
