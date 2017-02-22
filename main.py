import random


class MapReduce:
    def map(self, data):
        raise NotImplementedError()

    def reduce(self, key, values):
        raise NotImplementedError()

    def merge(self, data):
        grouped_data = {}

        for chunk in data:
            for (key, value) in chunk:
                if key in grouped_data:
                    grouped_data[key].append(value)
                else:
                    grouped_data[key] = [value]

        return grouped_data

    def execute(self, data):
        mapped_chunks = self.map(data)

        grouped_data = self.merge(mapped_chunks)

        results = [self.reduce(key, grouped_data[key]) for key in grouped_data]

        return results


class MaxMapReduce(MapReduce):
    def map(self, data):
        mapped_chunks = []

        for index, chunk in enumerate(data):
            mapped_chunks.append([(index, value) for value in chunk])

        return mapped_chunks

    def reduce(self, key, values):
        return max(values)

    def perform(self, data):
        result = self.execute(data)

        return max(result)


class AverageMapReduce(MapReduce):
    def map(self, data):
        mapped_chunks = []

        for index, chunk in enumerate(data):
            mapped_chunks.append([(index, value) for value in chunk])

        return mapped_chunks

    def reduce(self, key, values):
        return sum(values), len(values)

    def perform(self, data):
        result = self.execute(data)

        total_sum = sum([s for s, _ in result])
        total_num = sum([n for _, n in result])

        return total_sum / total_num


class UniqueMapReduce(MapReduce):
    def map(self, data):
        mapped_chunks = []

        for chunk in data:
            mapped_chunks.append([(value, 1) for value in chunk])

        return mapped_chunks

    def reduce(self, key, values):
        return key

    def perform(self, data):
        result = self.execute(data)

        return result


class UniqueQuantityMapReduce(UniqueMapReduce):
    def perform(self, data):
        result = self.execute(data)

        return len(result)


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
