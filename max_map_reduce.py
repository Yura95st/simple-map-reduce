from map_reduce import MapReduce


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
