# PyMapReduce

一个基于Python的简单的MapReduce实现，使用方法尽可能还原Hadoop平台的Java用例。

## Usage

```python
import pymapreduce

def example_mapper(item: Any) -> List[Tuple[Any, Any]]:
    words = item.split()
    return [(word, 1) for word in words]

def example_reducer(key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
    return [(key, sum(values))]

if __name__ == "__main__":
    data = ["hello world", "hello mapreduce", "world of mapreduce"]
    mr = MapReduce(example_mapper, example_reducer)
    result = mr.execute(data)
    print(result)
```

## LICENSE

GNU Generic Public License V3.0

