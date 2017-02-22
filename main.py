class MapReduce:
    @staticmethod
    def merge(data):
        grouped_data = {}

        for chunk in data:
            for (key, value) in chunk:
                if key in grouped_data:
                    grouped_data[key].append(value)
                else:
                    grouped_data[key] = [value]

        return grouped_data

    @staticmethod
    def map(data):
        return [(value, 1) for value in data]

    @staticmethod
    def reduce(key, values):
        return key

    def perform(self):


CHUNKS_COUNT = 3


def get_chunks(data, chunk_num):
    division = len(data) / chunk_num
    return [data[round(division * i):round(division * (i + 1))] for i in range(chunk_num)]


# #2
# def reduce(key, values):
#     return sum(values), len(values)

# def map(value, key):
#     return value, 1


# #1
# def reduce(key, values):
#     return max(values)


def main(chunks_count=3):
    data = [1, 1, 1, 2, 3, 4, 5, 5, 6, 6, 6]

    mapped_chunks = [MapReduce.my_map(chunk) for chunk in get_chunks(data, chunks_count)]

    grouped_data = MapReduce.merge(mapped_chunks)

    results = [MapReduce.reduce(key, grouped_data[key]) for key in grouped_data]

    print(results)
    print()
    print(len(results))


if __name__ == '__main__':
    main()
