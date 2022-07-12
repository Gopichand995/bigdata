def insertion_sort(elements):
    for i in range(1, len(elements)):
        anchor = elements[i]
        j = i - 1
        while j >= 0 and anchor < elements[j]:
            elements[j + 1] = elements[j]
            j -= 1
        elements[j + 1] = anchor


if __name__ == "__main__":
    sort_list = [11, 9, 13, 15, 7, 45, 36, 25]
    insertion_sort(sort_list)
    print(sort_list)
