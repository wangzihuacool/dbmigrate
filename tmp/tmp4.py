x = 10
expr = """
z = 30
x = 10
y = 20
sum = x + y + z
print(sum)
"""


def func():
    resp = {}
    y = 20
    x = 10
    exec(expr)
    exec(expr, {'x': 1, 'y': 2})
    exec(expr, resp)
    return resp

resp = func()
print(type(resp))
print(resp.get('x'))