---
id: functions-state
title: Pulsar Functions State Storage (Developer Preview)
sidebar_label: State Storage
---

Since Pulsar 2.1.0 release, Pulsar integrates with Apache BookKeeper [table service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f)
for storing the `State` for functions. For example, A `WordCount` function can store its `counters` state into BookKeeper's table service via Pulsar Functions [State API](#api).

## API

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
### Java API

Currently Pulsar Functions expose following APIs for mutating and accessing State. These APIs are available in the [Context](functions-api.md#context) object when
you are using [Java SDK](functions-api.md#java-sdk-functions) functions.

#### incrCounter

```java
    /**
     * Increment the builtin distributed counter refered by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);
```

Application can use `incrCounter` to change the counter of a given `key` by the given `amount`.

#### getCounter

```java
    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);
```

Application can use `getCounter` to retrieve the counter of a given `key` mutated by `incrCounter`.

Besides the `counter` API, Pulsar also exposes a general key/value API for functions to store
general key/value state.

#### putState

```java
    /**
     * Update the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);
```

#### getState

```java
    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);
```

#### deleteState

```java
    /**
     * Delete the state value for the key.
     *
     * @param key   name of the key
     */
```

Counters and binary values share the same keyspace, so this deletes either type of state.

<!--Python-->
### Python API

Currently Pulsar Functions expose following APIs for mutating and accessing State. These APIs are available in the [Context](functions-api.md#context) object when
you are using [Python SDK](functions-api.md#python-sdk-functions) functions.

#### incr_counter

```python
  def incr_counter(self, key, amount):
    """incr the counter of a given key in the managed state"""
```

Application can use `incr_counter` to change the counter of a given `key` by the given `amount`.
If the `key` does not exist, it is created.

#### get_counter

```python
  def get_counter(self, key):
    """get the counter of a given key in the managed state"""
```

Application can use `get_counter` to retrieve the counter of a given `key` mutated by `incrCounter`.

Besides the `counter` API, Pulsar also exposes a general key/value API for functions to store
general key/value state.

#### put_state

```python
  def put_state(self, key, value):
    """update the value of a given key in the managed state"""
```

The key is a string, and the value is arbitrary binary data

#### get_state

```python
  def get_state(self, key):
    """get the value of a given key in the managed state"""
```

#### del_counter

```python
  def del_counter(self, key):
    """delete the counter of a given key in the managed state"""
```

Counters and binary values share the same keyspace, so this deletes either type of state.

<!--END_DOCUSAURUS_CODE_TABS-->

## Query State

A Pulsar Function can use the [State API](#api) for storing state into Pulsar's state storage
and retrieving state back from Pulsar's state storage. Additionally Pulsar also provides
CLI commands for querying its state.

```shell
$ bin/pulsar-admin functions querystate \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <function-name> \
    --state-storage-url <bookkeeper-service-url> \
    --key <state-key> \
    [---watch]
```

If `--watch` is specified, the CLI will watch the value of the provided `state-key`.

## Example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->

{@inject: github:`WordCountFunction`:/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/WordCountFunction.java} is a very good example
demonstrating on how Application can easily store `state` in Pulsar Functions.

```java
public class WordCountFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) throws Exception {
        Arrays.asList(input.split("\\.")).forEach(word -> context.incrCounter(word, 1));
        return null;
    }
}
```

The logic of this `WordCount` function is pretty simple and straightforward:

1. The function first splits the received `String` into multiple words using regex `\\.`.
2. For each `word`, the function increments the corresponding `counter` by 1 (via `incrCounter(key, amount)`).

<!--Python-->

```python
from pulsar import Function

class WordCount(Function):
    def process(self, item, context):
        for word in item.split():
            context.incr_counter(word, 1)
```

The logic of this `WordCount` function is pretty simple and straightforward:

1. The function first splits the received string into multiple words on space
2. For each `word`, the function increments the corresponding `counter` by 1 (via `incr_counter(key, amount)`).

<!--END_DOCUSAURUS_CODE_TABS-->
