from collections import defaultdict
from functools import reduce
from typing import Callable, List, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

class MapReduce:
    def __init__(self, mapper: Callable, reducer: Callable, num_workers: int = 4):
        self.mapper = mapper
        self.reducer = reducer
        self.num_workers = num_workers
    
    def map(self, data: List[Any]) -> List[Tuple[Any, Any]]:
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.mapper, item) for item in data]
            results = [future.result() for future in as_completed(futures)]
        return [item for sublist in results for item in sublist]
    
    def shuffle(self, mapped_data: List[Tuple[Any, Any]]) -> defaultdict:
        shuffled_data = defaultdict(list)
        for key, value in mapped_data:
            shuffled_data[key].append(value)
        return shuffled_data
    
    def reduce(self, shuffled_data: defaultdict) -> List[Tuple[Any, Any]]:
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.reducer, key, values) for key, values in shuffled_data.items()]
            results = [future.result() for future in as_completed(futures)]
        return results
    
    def execute(self, data: List[Any]) -> List[Tuple[Any, Any]]:
        mapped_data = self.map(data)
        shuffled_data = self.shuffle(mapped_data)
        reduced_data = self.reduce(shuffled_data)
        return [res for lst in reduced_data for res in lst]

# Example usage
def example_mapper(item: Any) -> List[Tuple[Any, Any]]:
    words = item.split()
    return [(word, 1) for word in words]

def example_reducer(key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
    return (key, sum(values))

if __name__ == "__main__":
    data = ["hello world", "hello mapreduce", "world of mapreduce"]
    mr = MapReduce(example_mapper, example_reducer)
    result = mr.execute(data)
    print(result)

