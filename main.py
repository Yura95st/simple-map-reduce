import random
from max_map_reduce import MaxMapReduce
from average_map_reduce import AverageMapReduce
from unique_map_reduce import UniqueMapReduce
from unique_quantity_map_reduce import UniqueQuantityMapReduce


def get_chunks(data, chunk_num):
    n = len(data) / chunk_num

    return [data[round(n * i):round(n * (i + 1))] for i in range(chunk_num)]


def generate_data(n, max_value):
    return [random.randint(0, max_value) for _ in range(n)]


def main(chunks_count=3, n=10, max_value=3):
    data = generate_data(n, max_value)

    print(data)

    chunks = get_chunks(data, chunks_count)

    map_reduce_services = [MaxMapReduce(), AverageMapReduce(), UniqueMapReduce(), UniqueQuantityMapReduce()]

    for service in map_reduce_services:
        result = service.perform(chunks)

        print()
        print(result)


if __name__ == '__main__':
    main()
