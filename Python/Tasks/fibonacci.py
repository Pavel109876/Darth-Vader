fib1 = fib2 = 1

n = int(input('Please, input the number: '))

if n <= 0:
    print('The number must be more than 0')
elif n == 1:
    print('1')
else:
    print(fib1, fib2, end=' ')
    for i in range(2, n):
        fib_sum = fib1 + fib2
        fib1 = fib2
        fib2 = fib_sum
        print(fib_sum, end=' ')