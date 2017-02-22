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
