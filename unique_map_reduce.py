from map_reduce import MapReduce


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
