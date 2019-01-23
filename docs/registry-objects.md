# Registry Objects
Every application has to be connected to some kind of client at some point, either through a user interface toolkit or communication protocol. In Pybiz, this is what a `Registry` is for. These objects can be used to add native Python functions to a collection (i.e., registering them), through the use of a decorator, for the sake of defining an API. Each implementation encapsulates the logic necessary for decoding inputs received by the client, binding them to arguments expected by registered functions, and encoding their return values as output. These functions can be thought of as _endpoints_.

The `Registry` class can be extended to implement a wide range of API's, like command-line interface (CLI) programs, WSGI web apps, web socket servers, gRPC servers, Celery workers, and so on.

## Component Overview
When working with `Registry` types, there are three components to be aware of: the `Registry` itself, the `RegistryDecorator`, and the `RegistryProxy`. These components are tightly coupled but independently extensible.

### Registry

### RegistryDecorator

### RegistryProxy

## IO Marshaling
### Decoding & Binding Arguments
Each `Registry` is responsible for decoding inputs from a client and binding them to the arguments of registered functions. This is the primary function of the `Registry.on_request` method. For example, suppose we have a registry that interfaces with a client that, for some made up reason, sends and receives data as Python pickles. The implementation of `on_request` might look something like:

```python
def on_request(self, proxy, pickled, *args, **kwargs):
  data = pickle.loads(pickled)
  return (data['arg_list'], data['kwarg_dict'])
```

### Encoding Return Values
```python
def on_response(self, proxy, result, *args, **kwargs):
    return pickle.dumps(result)
```

## Function Decoration
When you need to take execute custom logic upon decorating each callable, you can implement the `on_decorate` method stub. For example:

```python
def on_decorate(self, proxy):
  do_something_custom(proxy)
```
