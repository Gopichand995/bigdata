import string


def print_rangoli(size):
    if 0 < size < 27:
        # your code goes here
        for i in range(2 * size - 1):
            if i < size - 1:
                print('-' * (2 * (size - 1 - i)) + '-'.join(
                    [string.ascii_lowercase[size - i - 1] for i in range(i + 1)] + [string.ascii_lowercase[size - i] for
                                                                                    i in reversed(
                            range(1, i + 1))]) + '-' * (2 * (size - 1 - i)))
            elif i == size - 1:
                print('-'.join([string.ascii_lowercase[size - i - 1] for i in range(size)]) + '-' + '-'.join(
                    [string.ascii_lowercase[j] for j in range(1, size)]))
            else:
                print('-' * (2 * (i - size + 1)) + '-'.join(
                    [string.ascii_lowercase[size - i - 1] for i in range(2 * size - i - 1)] + [
                        string.ascii_lowercase[size - i] for i in reversed(range(1, 2 * size - i - 1))]) + '-' * (
                              2 * (i - size + 1)))


if __name__ == '__main__':
    n = 26
    print_rangoli(n)


# import string
# alpha = string.ascii_lowercase
#
# n = int(input())
# L = []
# for i in range(n):
#     s = "-".join(alpha[i:n])
#     L.append((s[::-1]+s[1:]).center(4*n-3, "-"))
# print('\n'.join(L[:0:-1]+L))

# Enter your code here. Read input from STDIN. Print output to STDOUT
# s = input()
# n = len(s)
# i = 1
# count = 1
# final = []
# while i < n:
#     if s[i] == s[i-1]:
#         if i == n-1:
#             count += 1
#             final.append((count, int(s[i-1])))
#             break
#         count += 1
#         i += 1
#         continue
#     final.append((count, int(s[i-1])))
#     count = 1
#     i += 1
# print(*final, sep=' ')

