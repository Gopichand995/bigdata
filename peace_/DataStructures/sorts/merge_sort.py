# sorting the same array without the usage of "sorted_list"
def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left_arr = merge_sort(arr[:mid])
    right_arr = merge_sort(arr[mid:])
    return merge_two_sorted_lists(left_arr, right_arr, arr)


def merge_two_sorted_lists(a, b, arr):
    len_a = len(a)
    len_b = len(b)
    i = j = k = 0
    while i < len_a and j < len_b:
        if a[i] <= b[j]:
            arr[k] = a[i]
            i += 1
        else:
            arr[k] = b[j]
            j += 1
        k += 1
    while i < len_a:
        arr[k] = a[i]
        i += 1
        k += 1
    while j < len_b:
        arr[k] = b[j]
        j += 1
        k += 1
    return arr


if __name__ == "__main__":
    arr = [
        [],
        [0],
        [1, -2, 3],
        [3, 2, -1],
        [21, 16, 18, -26, 45, 65, 36, 20, 10]
    ]
    for i in range(len(arr)):
        print(merge_sort(arr[i]))




# def merge_sort(arr):
#     if len(arr) <= 1:
#         return arr
#     mid = len(arr) // 2
#     left_arr = merge_sort(arr[:mid])
#     right_arr = merge_sort(arr[mid:])
#     return merge_two_sorted_lists(left_arr, right_arr)
#
#
# def merge_two_sorted_lists(a, b):
#     sorted_list = []
#     len_a = len(a)
#     len_b = len(b)
#     i = j = 0
#     while i < len_a and j < len_b:
#         if a[i] <= b[j]:
#             sorted_list.append(a[i])
#             i += 1
#         else:
#             sorted_list.append(b[j])
#             j += 1
#     while i < len_a:
#         sorted_list.append(a[i])
#         i += 1
#     while j < len_b:
#         sorted_list.append(b[j])
#         j += 1
#     return sorted_list
#
#
# if __name__ == "__main__":
#     arr = [
#         [21, 16, 18, 26, 45, 65, 36, 20, 10],
#         [],
#         [1],
#         [1, 2, 3],
#         [3, 2, 1]
#     ]
#     for i in range(len(arr)):
#         print(merge_sort(arr[i]))
