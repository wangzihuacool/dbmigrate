from multiprocessing import Pool

def f(x):
    print(x)
    return x.get('age')

if __name__ == '__main__':
    test_list = [{'name': 'test1', 'age': 24}, {'name': 'test2', 'age': 18}, {'name': 'test3', 'age': 16}]
    with Pool(5) as p:
        result = p.map(f, test_list)
    print(result)