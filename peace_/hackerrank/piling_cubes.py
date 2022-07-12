# https://www.hackerrank.com/challenges/piling-up/problem
# Enter your code here. Read input from STDIN. Print output to STDOUT


def cube(arr):
    n = len(arr)
    if n == 1 or n == 2:
        return "Yes"
    if arr[0] <= arr[n - 1]:
        if arr[n - 2] > arr[n - 1]:
            return "No"
        return cube(arr[:n - 1])
    if arr[0] > arr[n - 1]:
        if arr[1] > arr[0]:
            return "No"
        return cube(arr[1:n])


T = int(input())
for _ in range(T):
    len_ = input()
    data_ = list(map(int, input().split(' ')))
    print(cube(data_))
