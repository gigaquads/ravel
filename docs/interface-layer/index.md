# Application Interface Layer

### Table of Contents
- [Introduction](#introduction)
- [The Registry System](#./registry.md)
- [Builtin Registry Types](#./builtin-registry-types.md)
- [Middleware](#./middleware.md)

---
At some point, every application needs an API or user interface. To this end, Pybiz provides a high-level interface abstraction that insulates business logic and makes it possible to share the same Python function as an endpoint or callback in a range of different API's and user interfaces simultaneously.

Let's look at an example of a function registered with three distinct interface abstractions.

```python
@cli()
@web(http_method='GET', url_path='/users/{user_id}')
@rpc(request={'user_id': Int()}, response=User.schema)
def get_user(user_id):
  return User.get(user_id)
```

In this example, we have registered `get_user` with three distinct interfaces: a command-line interface (CLI) application, a web server, and a gRPC service. Note that, in doing so, we made no reference to anything related to CLI, HTTP, or RPC in the arguments or body of the function.

All three decorators derive from the a common Pybiz base type, called `Registry`, sharing the same configuration interface and lifecycle methods. In sections that follow, we will look at this and related classes in more detail.
