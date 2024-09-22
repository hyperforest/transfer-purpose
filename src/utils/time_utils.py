from datetime import datetime


def timeit(func):
    def wrapper(*args, **kwargs):
        name = func.__name__
        start = datetime.now()

        result = func(*args, **kwargs)

        runtime = (datetime.now() - start).total_seconds()
        print(f'[INFO] {datetime.now()} - {runtime:.2f}s - {name}')

        return result

    return wrapper
