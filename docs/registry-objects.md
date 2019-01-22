# Registry Objects
Pybiz provides a generic and extensible way to collect Python callables into a collection, known as a `Registry`, through use of a decorator.

Many different types of applications use (or could use) the decorator pattern. They all do essentially the same thing; that is, they register a set of callables as endpoints, whether for HTTP, command-line interface, RPC, desktop application, or other kinds of endpoints. In short, the `Registry` class defines an application API.

## Overview
#### Argument Packing and Unpacking
Each `Registry` is responsible for unpacking raw request data formats received through some protocol or interface into the arguments and keyword arguments expected by the native Python callables decorated by the registry. Likewise, it is also responsible for packing the native Python values returned by these callables into the appropriate response format.

This is the primary function of the `on_request` and `on_response` methods on the base `Registry` class. For example, suppose we have a registry that interfaces with a client that sends and receives data as Python pickles. The implementation of these methods might look something like:

```python
def on_request(self, proxy, pickled, *args, **kwargs):
  data = pickle.loads(pickled)
  return (data['arg_list'], data['kwarg_dict'])

def on_response(self, proxy, result, *args, **kwargs):
    return pickle.dumps(result)
```

#### Custom Registration Logic
When you need to take execute custom logic upon decorating each callable, you can implement the `on_decorate` method stub. For example:

```python
def on_decorate(self, proxy):
  do_something_custom(proxy)
```
