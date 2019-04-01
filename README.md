This is port of avro java library(1.8.2)


[Notes]()

1. Can not define name again in schema


















### Extensions

1. Set
2. LRU Map
3. Date as String



#### Set

[Notes]()

- Set is only supported for GenericRecord Type.
- Data from set will be encoded as array of values .
- String types are decoded as Utf8 class . Call toString() to convert to string.
```java
    final Set<Utf8> set = (Set) readRecord.get("ids");
    for (Utf8 s : set) {
      final String value = s.toString();
      System.out.println(ids.contains(value));
    }
```


- Set is not integrated as typed Schema Builder code.
- Code generation is not supported for set type.

[Todo]()

- [x] Support Set Write
- [x] Support Set Read
- [ x ] Test Schema Resolution for set (read/write)
- [ ] Compatibility test with rust library


#### LRU Map

[Notes]()

[Todo]()

- [ ] Support LRU Map Write
- [ ] Support LRU Map Read
- [ ] Test Schema Resolution for LRU Map (read/write)
- [ ] Support Time based LRU
- [ ] Support Count based LRU
- [ ] Compatibility test with rust library