PyBiz
================================================================================
You can think of PyBiz as an ORM for web frameworks, not databases. It abstracts
out the app from the framework. The architecture of PyBiz application has a
traditional design with three layers.

## The Service Layer
PyBiz provides annotations for request validation, authorization, and
integration wth existing web frameworks. Toggling between Django, Pyramid, and
Falcon is as easy as changing one line of code.

## The Business Layer
In addition, PyBiz also contains components for so-called business objects.  In
a nutshell, business objects should contain all logic that pertains to the
business domain of the application. The business layer makes no reference HTTP
requests or database queries. It's made up of classes with names like
`Account`, `User`, `Project`, etc. These components should be written so that
they read as much like user stories as possible when composed.

## The Data Access Layer
The DAL should be familiar to anyone working with Java Frameworks. Its
responsibility is to define the interface to an abstract data store, be it
implemented with MySQL, Redis, another micro-service, or something else.

_More to come..._
