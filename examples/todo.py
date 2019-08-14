import pybiz

app = pybiz.app.Repl()


class Task(pybiz.BizObject):
    name = pybiz.String(nullable=False)
    list_id = pybiz.Field(private=True)
    position = pybiz.Int(nullable=False)
    status =  pybiz.Enum(
        pybiz.String(), values=('todo', 'doing', 'done'),
        default='todo', nullable=False
    )


class TaskList(pybiz.BizObject):
    name = pybiz.String()
    size = pybiz.Int(nullable=False, default=lambda: 0)
    tasks = pybiz.Relationship(
        join=lambda thing: (TaskList._id, Task.list_id),
        order_by=lambda thing: Task.position.asc,
        many=True,
    )


@app()
def new_list(tasks: TaskList) -> TaskList:
    return tasks.create()


@app()
def get_list(tasks: TaskList) -> TaskList:
    return tasks.load({'*', 'tasks.*'})


@app()
def add_task(tasks: TaskList, task: Task) -> Task:
    task = task.merge(position=tasks.size, list_id=tasks._id).create()
    tasks.size += 1
    tasks.update()
    return task


if __name__ == '__main__':
    app.bootstrap(
        manifest={
            'package': 'todo',
            'bootstraps': [
                {'dao': 'FilesystemDao', 'params': {'root': '/tmp/todo-app'}}
            ],
            'bindings': [
                {'biz': 'Task', 'dao': 'FilesystemDao'},
                {'biz': 'TaskList', 'dao': 'FilesystemDao'},
            ]
        },
    ).start()
