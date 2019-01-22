# Data Access Objects
Business objects implement a CRUD interface, which means that data needs to be stored and accessed somehow. Depending on your needs, this can mean in-memory storage, filesystem storage, a database, a server, or any number of other things. In pybiz, a _data access object_ (DAO) is implements a CRUD interface for a particular persistence technology. The abstract base `Dao` class is defined in `pybiz.dao.base`.

## DAO Initialization
There are two main places where initialization logic can take place. The first place is the constructor. The second is a method, called `bind`, which Pybiz invokes internally when initializing a new `Bizobject` class. The `bind` method associates each `BizObject` class with a singleton instance of some `Dao`.

When overriding this method, be sure to call `super().bind(biz_type)`.

Here is the base bind method:

```python
def bind(self, bizobj_type: Type[BizObject]) -> None:
  self.bizobj_type = bizobj_type
```

From `bind`, you can inspect the `BizObject` class with which the `Dao` instance is being bound to initialize any schema or storage configuration for this kind of business object.

The `Dao` instance can be accessed either through instance attribute or class method, like below:

```python
dao_from_class = User.get_dao()
dao_from_instance = user.dao

assert dao_from_class is dao_from_instance
```

For details on how `Dao` objects are associated with `BizObject` classes, see the section on application [Configuration](tod).

## DAO Interface
Every `Dao` subclass must implement the abstract base interface. The following is an overview of these methods and how they are expected to work.

### Query
```python
def query(self, predicate, fields=None, **kwargs) -> List[Dict]:
  pass
```
The `query` method is by far the most complex method to implement but also the most rewarding. The first argument is a `Predicate` object. The purpose of this method is to provide SQL-like `select` statement functionality. See: [Business Objects](todo) for details on what predicates are and how to use them.

### Fetch & Fetch Many
```python
def fetch(self, _id, fields: List[Text] = None) -> Dict:
  pass

def fetch_many(self, _ids: List, fields: List[Text] = None) -> Dict:
  pass
```

Fetch one or more records. If the `fields` keyword argument is `None`, the entire record should be returned; otherwise, only the specified fields should be returned. For `fetch_many`, along with `create_many` and `update_many`, the return value should be a dict, having `_id` keys and dict records as values. In other words,

```python
records = dao.fetch_many(_ids=[1, 2])
for k, v in records.items():
  assert k == v['_id']
```

### Create & Create Many
```python
def create(self, record: Dict) -> Dict:
  pass

def create_many(self, records: List[Dict]) -> Dict:
  pass
```

You are relatively unrestricted in how you implement the `Dao` interface; however, there is one important requirement for `create` and `create_many`, and that is that these methods must generate an ID for each record. To illustrate with an example, consider this:

```python
record = dao.create({'name': 'Picard'})
assert record.get(_id) is not None
```

### Update & Update Many
```python
def update(self, _id, values: Dict) -> Dict:
  pass

def update_many(self, _ids: List, values: Dict) -> Dict:
  pass
```

Updating works the same way as creating, with the exception that the `_id` for each record being updated is assumed to exist.

### Delete & Delete Many
```python
def delete(self, _id) -> None:
  pass

def delete_many(self, _ids: List) -> None:
  pass
```

You are free to implement deletion however you see fit, whether it means removing a row from a database or simply setting an `is_deleted` flag.

### Next ID
```python
def next_id(self, record: Dict) -> object:
  pass
```

The purpose of `next_id` is to give the Pybiz application a chance to generate the next `_id` for a new record being created. This method may return `None`, which is a signal that the underlying persistence technology is expected to create and return the `_id` instead.

#### Example 1
Using a random UUID hex string

```python
def next_id(self, record):
  return uuid.uuid4().hex
```

#### Example 2
Using a value in the new record

```python
def next_id(self, record):
  return record['name'].lower().replace(' ', '_')
```

#### Example 3
Using a persistent counter

```python
def next_id(self, record):
  return redis.incr('id-counter')
```
## Built-in DAO Types
A handful of base `Dao` types are available in Pybiz. These cover some basic use-cases, like storing things in memory, the filesystem, relational databases, and in Redis. This section provides a concise overview of how these `Dao` types work.

### Memory DAO
The `MemoryDao` is the default `Dao` type. Internally, records are stored in in-memory. Each field defined by the `BizObject` backed by this class is indexed using an in-memory B-tree data structure.

#### Runtime Complexity
Note that the approximate complexity of `MemoryDao.query` is `O(log N)`. The exact runtime is a function of the number of conditional predicates used in the query and the number of matching records for each one. The algorithm is a recursive procedure, in which each conditional predicate generates a set of matching _ids, and each boolean predicate applies a set intersection of union operation.

#### Use Cases
- Initial rapid development of application domain logic
- Interactive testing and development in a REPL
- Unit and integration testing

### Filesystem DAO
The `FilesystemDao` persists each record as a file in the local filesystem. The file format can be specified in the constructor and defaults to YAML.

Any file type class derived from the base `File` type in the `appyratus` package can be used to indicate what file type to use in the DAO, like

```python
yaml_dao = FilesystemDao(ftype=Yaml)
json_dao = FilesystemDao(ftype=Json)
text_dao = FilesystemDao(ftype=Text)
```

Using `query` on a `FilesystemDao` works by reading _all_ files into memory and delegating to the `query` method of a `MemoryDao`. If there are many thousands or millions of records, you may experience some drag upon first call.

#### Use Cases
- Managing CSV, YAML, and JSON files, intended for human consumption.
- Managing YAML files used by other programs, like Kubernetes, Ansible, etc.

### Sqlalchemy DAO
For most production applications, you will inevitably want to store data in a relational database, like MySQL or PostgreSQL. This is when `SqlalchemyDao` comes in handy. Each instance manages a single table or view, storing a record for each `BizObject` instance to which it is bound. Therefore, you must use something like a database view if you must perform low-level SQL joins in order to gather together the all field values expected by the business object.

#### Sqlalchemy Profiles
In case multiple `pybiz` applications are loaded into memory at the same time, we need a way to manage the underlying connection pools and Sqlalchemy metadata for each one separately. For this purpose, we have the notion of a "profile". Upon application initialization, you must create a new `Profile` object, like so:

```python
profile = SqlalchemyDao.create_profile('my-app-name')

# From here, you can do things like,
# profile.create_tables()
```

#### Sqlalchemy Interface
Generally speaking, setting up the `SqlalchemyDao` for use in an Pybiz application consists of two steps: (1) initializing the underlying connection pool, i.e. "engine" at startup time, and (2) acquiring and managing a database connection within the scope of a request.

We can use middleware for this, doing something like this:

```python
class SqlalchemyTransactionMiddleware(RegistryMiddleware):
  def __init__(self, profile: Text):
    self.profile = SqlalchemyDao.profiles[profile]
    self.profile.initialize()
    self.session = None

  def pre_request(self, proxy, args, kwargs):
    self.profile.connect()
    self.session = self.profile.begin()

  def post_request(self, proxy, args, kwargs, result):
    if self.session is None:
      return
    try:
      self.session.commit()
    except:
      self.session.rollback()
    finally:
      self.session.close()
```

See [Middleware](todo) for details on how to integrate middleware with applications.

#### Use Cases
- Production applications in general
- Things that requires efficient ad-hoc queries
- Things that require fault-tolerance, ACID compliance

### Redis DAO
The `RedisDao` is most similar to the `MemoryDao`, with the obvious exception that records and indexes are stored in a Redis database. The algorithm for the `query` method is essentially identical.

#### Use Cases
- ACID compliance not essential
- Need persistence but want to go "schemaless"
- Good for smaller, more flat business objects
- Good for http session storage and things that need speed
