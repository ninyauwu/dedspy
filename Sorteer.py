import sys

array = [20, 19, 41, 3, 4, 23, 49, 14, 21, 6, 1, 7, 36, 24, 22, 42, 31, 12, 47, 2, 7]


def calc_median(arr):
    if len(arr) % 2.0 == 1.0:
        return arr[int((len(arr) / 2.0) + 0.5)]


def quicksort_up(arr):
    if len(arr) < 1:
        return []
    left_arr = []
    right_arr = []
    pivot = arr.pop(len(arr) - 1)
    for x in arr:
        if x <= pivot:
            left_arr.append(x)
        else:
            right_arr.append(x)
    left_arr = quicksort_up(left_arr)
    left_arr.append(pivot)
    right_arr = quicksort_up(right_arr)
    return left_arr + right_arr

def quicksort_down(arr):
    if len(arr) < 1:
        return []
    left_arr = []
    right_arr = []
    pivot = arr.pop(0)
    for x in arr:
        if x <= pivot:
            left_arr.append(x)
        else:
            right_arr.append(x)
    left_arr = quicksort_up(left_arr)
    left_arr.append(pivot)
    right_arr = quicksort_up(right_arr)
    return left_arr + right_arr


def swap(arr, i, j):
    temp = arr[i]
    arr[i] = arr[j]
    arr[j] = temp


def bubblesort(arr):
    count = 0
    limit = len(arr)
    while limit > 1:
        for i in range(1, limit):
            if arr[i] < arr[i - 1]:
                swap(arr, i - 1, i)
            i += 1
            count += 1

        limit -= 1

    print(count)
    return arr


def counting_sort(arr, exp1):
    size = len(arr)

    # The output array elements that will have sorted arr
    output = [0] * size

    # initialize count array as 0
    count = [0] * 10

    # Store count of occurrences in count[]
    for i in range(0, size):
        index = arr[i] // exp1
        count[index % 10] += 1

    # Change count[i] so that count[i] now contains actual
    # position of this digit in output array
    for i in range(1, 10):
        count[i] += count[i - 1]

    # Build the output array
    i = size - 1
    while i >= 0:
        index = arr[i] // exp1
        output[count[index % 10] - 1] = arr[i]
        count[index % 10] -= 1
        i -= 1

    # Copying the output array to arr[],
    # so that arr now contains sorted numbers
    i = 0
    for i in range(0, len(arr)):
        arr[i] = output[i]


def radix_sort(arr):
    # Find the maximum number to know number of digits
    max1 = max(arr)

    # Do counting sort for every digit. Note that instead
    # of passing digit number, exp is passed. exp is 10^i
    # where i is current digit number
    exp = 1
    while max1 / exp >= 1:
        counting_sort(arr, exp)
        exp *= 10

    return arr


print(bubblesort(array))

