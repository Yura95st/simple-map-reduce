from map_reduce import MapReduce


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
