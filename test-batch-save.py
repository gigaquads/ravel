import ravel


class Person(ravel.Resource):
    name = ravel.String()
    parent_id = ravel.Id(lambda: Person)

    @ravel.relationship(lambda: (Person.parent_id, Person._id))
    def parent(self, request):
        return request.result

    @ravel.relationship(lambda: (Person._id, Person.Batch.parent_id))
    def children(self, request):
        return request.result



app = ravel.Application()
app.bootstrap(namespace=locals())

parent = Person(name='parent').save()
children = Person.Batch()
for i in range(1, 3):
    children.append(Person(name=f'child {i}', parent_id=parent._id))
children.save()

children[0].parent.name = 'dummy'
children.save()
assert parent.resolve('name').name == 'parent'

children[0].parent.name = 'dummy'
children.save('parent')
assert parent.resolve('name').name == 'dummy'

# TODO: save batch of batches
