from unique_map_reduce import UniqueMapReduce


class UniqueQuantityMapReduce(UniqueMapReduce):
    def perform(self, data):
        result = self.execute(data)

        return len(result)
