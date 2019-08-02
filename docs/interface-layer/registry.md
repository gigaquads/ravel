# Api Applications

- [Overview](#overview)
- [Decorators and Proxies](#decorator-and-proxy-objects)
- [Walkthru](#api-walkthru)
    1. [Implementing `Api`](#step-1-implementing-api)
    2. [Registering Functions](#step-2-registering-functions)
    1. [Creating an Entrypoint](#step-3-creating-an-entrypoint)
    4. [Running the Program](#step-4-running-the-program)
- [Available Implementations](#available-implementations):
  - [Command Line Interface (CLI) Application]()
  - [IPython REPL Session]()
  - [Abstract Base HTTP Server]()
    - [Standard Library HTTP Server]()
    - [Abstract Base WSGI Service]()
      - [Falcon WSGI Service]()
  - [Abstract Base Async Task Server]()
    - [Web Socket Server]()

## Overview
In Pybiz, each `Api` manages a collection of Python functions that together define a high-level interface. Each function is added to the `Api` through a decorator. Moreover, it is job of the `Api` to provide middleware, IO marshaling logic, and a standard configuration interface and runtime environment.

Besides the `Api` class itself, there are two others that work in close concert with it. Here is an overview of their roles and responsibilities:

##### 1. `Api`
The `Api` manages a collection of `Proxy` objects, each of which corresponds to a decorated function, and implements key data transformation methods.

##### 2. `ApiDecorator`
Calling a `Api` object like a function creates a `ApiDecorator`, which in turn, wraps the decorated function in a `Proxy` and adds it to the `Api`.

##### 3. `Proxy`
Each `Proxy` is a callable object, which delegates calls to one of the decorated functions, executing middleware and IO marshaling.

---
These three classes can be illusstrated in a short example.


```python
# file: example.py

from pybiz.api.cli import CliApi

cli = CliApi()

@cli()                              # See Comment 1.
def echo(message):
  print('> ' + message.upper())

assert 'echo' in cli.proxies        # See Comment 2.

if __name__ == '__main__':          # See Comment 3.
  cli.manifest.process()
  cli.start()
```

###### Comments
1. Builds a ApiDecorator, which builds a Proxy and adds it to `cli`.
2. Proves that the `echo` function was registered as a `Proxy`.
3. Defines the program point of entry.

---
When we run this program, specifying the name of the `echo` command, followed by its arguments, the `CliApi` executes the corresponding `Proxy`, which in turn executes the plain `echo` function it wraps, in between middleware and IO.

This is what running the CLI program would look like:

```bash
$ example echo 'spam'
> SPAM
```

## Decorator and Proxy Objects
Functions are registered by using a `Api` instance as a decorator. The end result of registering a function is a `Proxy` object, which can access the arguments passed into the decorator, provided any were passed.

Consider the following example:

```python
web = WebServiceApi()

@web(method='GET', path='/test')
def test():
  pass
```

Here, `web` is our `Api` instance. By using it as a decorator, a `ApiDecorator` is created, which in turn creates a `Proxy`. The `ApiDecorator` holds references to both positional and keyword arguments -- namely, `method` and `path` in our example -- which can be accessed via `FunctionProxy` like so,

```python
proxy = web.proxies['test']

assert not proxy.decorator.args
assert proxy.decorator.kwargs == {'method': 'GET', 'path': '/test'}
```

This comes into play when derive the base `Proxy` to create something that looks and behaves more like an `HttpRoute` or `CliCommand` object, for example. To illustrate, we might do

```python
class HttpDecorator(ApiDecorator):
  pass

class HttpRoute(Proxy):

  @property
  def method(self):
    return self.decorator.kwargs.get('method', '').upper()

  @property
  def path(self):
    return self.decorator.kwargs.get('path')
```

You can indepdently override which `ApiDecorator` and `Proxy` types are used by a `Api` type by overriding the `decorator_type` and `proxy_type` instance properties.

```python
# inside class HttpApi(Api):

@property
def decorator_type(self) -> Type[ApiDecorator]:
    return HttpDecorator

@property
def proxy_type(self) -> Type[Proxy]:
    return HttpRoute
```

## Walkthru
We will walk through the creation and use of a fictitous `Api` class, which defines a CLI application for processing YAML files. We will look at how functions are registered as "commands" and are run from the shell. Let's imagine that the Python package for this project is called `yamlatrix`

### Step 1: Implementing `Api`
At a basic level, deriving the abstract `Api` base class involves implementing the `on_request`, `on_response`, `on_decorate`, and `start` abstract base methods.

In overview,

- **`on_request`** marshals requests into args and kwargs.
- **`on_response`** marshals out return values.
- **`on_decorate`** defines custom decoration logic.
- **`start`** is the main program entrypoint.

Here is the implementation:

```python
# file: processor.py

import os
import yaml
import json
import argparse

from pybiz.api.api import Api


class YamlProcessor(Api):
    def on_request(self, proxy, *args, **kwargs):
        """
        Read the input YAML file into a `data` argument and load the
        payload JSON object into keyword arguments.
        """
        path, payload = args[:2]
        print(f'>>> Reading: {os.path.abspath(path)}')
        with open(path) as yaml_file:
            data = yaml.load(yaml_file) or {}
        args = (path, data)
        kwargs = json.loads(payload) if payload else {}
        return (args, kwargs)

    def on_response(self, proxy, result, *args, **kwargs):
        """
        Write the output object back to the same YAML file path.
        """
        path = args[0]
        print(f'>>> Writing: {os.path.abspath(path)}')
        if result is not None:
            with open(path, 'w') as yaml_file:
                yaml.dump(result, yaml_file, default_flow_style=False)

    def on_decorate(self, proxy):
        """
        Perform some logic when the decorator is applied to each function.
        """
        print(f'>>> Registered command: "{proxy.name}"')

    def start(self):
        """
        Start defines the main program entrypoint.
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('command')
        parser.add_argument('path')
        parser.add_argument('--payload')

        # parse command line arguments
        args = parser.parse_args()

        # lookup and call proxy or else print help message
        proxy = self.proxies.get(args.command.lower())
        if proxy is not None:
            proxy(args.path, args.payload)
        else:
            print()
            print('Available Commands:')
            print('-' * 50)
            for name, proxy in self.proxies.items():
                print(f'{name:15} - {proxy.doc}')
```

###### Remarks
- `start` creates a command-line program using argparse and delegates calls to the appropriate proxy. If no proxy is found, it prints a help message.

- `on_request` loads a YAML file, translating the arguments received by the proxy into the arguments expected by the native Python `echo` function.

- `on_response` writes the output dict back to disk, provided there is a return value.

- `on_decorate` logs something, for example.

### Step 2: Registering Functions
After defining our `Api` class, we can create an instance and register some functions via decorator. Here are three such functions.

```python
# file: api.py

from yamlatrix.processor import YamlProcessor

app = YamlProcessor()


@app()
def update(path, data, **updates):
    """
    Merge input data into YAML file
    """
    data.update(updates)
    return data

@app()
def jsonify(path, data, **payload):
    """
    Save YAML to JSON file
    """
    dest = os.path.splitext(path)[0] + '.json'
    with open (dest, 'w') as json_file:
        json.dump(data, json_file, indent=2, sort_keys=True)

@app()
def show(path, data, **payload):
    """
    Pretty print the YAML file
    """
    print()
    print(
        yaml.dump(
            data,
            default_flow_style=False,
            explicit_start=True,
            explicit_end=True
        )
    )
    print()

```

### Step 3: Creating an Entrypoint
Now that we've registered some functions, we can define an entrypoint. An entrypoint is simply a "main" function that is executed either directly by us or by some other process. At a minimum, this usually requires just two lines of code.

```python
#!/usr/bin/env python3
# file: /usr/local/bin/yamlatrix

from yamlatrix.api import app

app.manifest.process()
app.start()
```

- The first line bootstraps the application. See [Manifest](todo) for more information.
- The second line invokes the entrypoint.

### Step 4: Running the Program
If our entrypoint file is called `yamlatrix` and is executable, and assuming that a `example.yaml` file in our working directory, we can invoke any function registered as a command. Here are some examples.

```bash
# add a field to the existing example.yaml file
$ yamlatrix update example.yaml --payload '{"foo": "bar"}'

# pretty preint the yaml file to stdout
$ yamlatrix show example.yaml

# copy the YAML file to a JSON file with the same name
$ yamlatrix jsonify example.yaml
$ cat example.json
```
## Available Implementations
Pybiz comes packaged with a handful of useful `Api` implementations that cover a range of applications. In this section, we will briefly look at each one.

### Command Line Interface (CLI) Application
- Module: `pybiz.api.cli`
- Api: `CliApi`
