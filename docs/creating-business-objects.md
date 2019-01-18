Define a business object is simple. Here's an example:

```python
from pybiz.biz import BizObject
from pybiz.schema import String, Int

class Album(BizObject):
  name = String()
  year = Int()

```

Now we can import `Album` into an interactive shell and start having some fun. To start, let's make some albums!

```python
In [1]: albums = [
   ...:     Album(name='Rally Around the BizObject', year=1999),
   ...:     Album(name='Embryonic Embrace', year=2014)
   ...: ]

In [2]: print(albums)
Out[2]: [<Album(?)*>, <Album(?)*>]
```

Let's dissect the way each `Album` object is represented in the shell: namely,  `<Album(?)*>`.

The first thing to notice is the question mark. This means that the album object is brand new and, as a consequence, doesn't have an ID. It is only by calling `album.save()` that an ID would generated for the new album.

Second, note the asterisk. This means that there are "dirty" fields on the album object, which will be flushed to the data store when calling `album.save`. Whenever a new object is created, _all_ fields are considered dirty by default. For existing objects, only fields modified at runtime are marked dirty.

Let's save these albums.

```python
In [4]: for album in albums:
    ...:     album.save()
    ...:

In [5]: albums
Out[5]: [<Album(1)>, <Album(2)>]
```

The above could also have been accomplished using shorthand, like:

```python
In [1]: albums = [
   ...:     Album(name='Rally Around the BizObject', year=1999).save(),
   ...:     Album(name='Embryonic Embrace', year=2014).save()
   ...: ]
```

Notice how the question marks have been replaced with the newly assigned ID's. In addition, the asterisks are gone. Now that we have persisted some albums, we can write some basic queries, like:

```python
In [6]: Album.query(Album.year > 2000, first=True)
Out[6]: <Album(2)>

In [7]: Album.query((Album.year > 2000) | (Album.name == 'Rally Around the BizObject'))
Out[7]: [<Album(1)>, <Album(2)>]
```

Queries written on boilerplate business objects like above are executed in a relatively efficient manner. Behind the scenes an in-memory B-Tree index is created for each scalar field defined on the class.

If we edit, say, the name of an album and want to save the change, we simply call `album.save` again. Here's an example:

```python
In [8]: album = albums[0]

In [9]: album.year = 2000

In [10]: album
Out[10]: <Album(1)*>  # notice the *

In [11]: album.save()
Out[11]: <Album(1)>
```

All CRUD operations are available out-of-the box, like:

```python
# fetch a single Album by ID
Album.get(_id=1)

# Fetch multiple albums
Album.get_many(_ids={1, 2})

# Delete the album
album.delete()
```

Next, we'll have a look at how relationships are defined between business objects.
