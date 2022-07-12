# as the bubble movies from botton to water surface this sort gets the name
# the first loop i runs "n-1" times
# the second loop j includes "n-1-i" in order to reduce complexity(leave i last elements for comparision)


def bubble_sort(elements):
    n = len(elements)
    swapped = False
    for i in range(n - 1):
        for j in range(n - 1 - i):
            if elements[j] > elements[j + 1]:
                tmp = elements[j]
                elements[j] = elements[j + 1]
                elements[j + 1] = tmp
                swapped = True
        if not swapped:
            break
    return elements


if __name__ == "__main__":
    list_to_sort = [12, 4, 56, 100, 25, 36, 9, 16, 49, 36]
    list_after_sort = bubble_sort(list_to_sort)
    print(list_after_sort)
