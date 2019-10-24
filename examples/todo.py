from pprint import pprint
import pybiz

app = pybiz.app.Repl()


class Task(pybiz.BizObject):
    name = pybiz.String(nullable=False)
    list_id = pybiz.Field()
    position = pybiz.Int(nullable=False)
    status =  pybiz.Enum(
        pybiz.String(), values=('todo', 'doing', 'done'),
        default='todo', nullable=False
    )
    task_list = pybiz.Relationship(
        join=lambda Task: (Task.list_id, BetterTaskList._id)
    )


class TaskList(pybiz.BizObject):
    name = pybiz.String()
    size = pybiz.Int(nullable=False, default=lambda: 0)
    tasks = pybiz.Relationship(
        join=lambda TaskList: (TaskList._id, Task.list_id),
        order_by=lambda Task: Task.position.asc,
        many=True,
    )
    final_task = pybiz.Relationship(
        join=lambda TaskList: Task,
        where=lambda Task, task_list: Task.position == (task_list.size - 1),
    )


class BetterTaskList(TaskList):
    pass


@app()
def new_list(tasks: BetterTaskList) -> TaskList:
    return tasks.create()


@app()
def get_list(tasks: BetterTaskList) -> TaskList:
    return tasks.load({'*', 'tasks.*'})


@app()
def add_task(tasks: BetterTaskList, task: Task) -> Task:
    task = task.merge(position=tasks.size, list_id=tasks._id).create()
    tasks.size += 1
    tasks.update()
    return task


if __name__ == '__main__':
    app.bootstrap(namespace=globals())

    task_lists = BetterTaskList.BizList()
    for i in range(2):
        task_list = new_list(BetterTaskList(name='todo list'))
        task = add_task(task_list, Task(name='do something', position=0))
        task = add_task(task_list, Task(name='do something else', position=1))
        task_lists.append(task_list)

    task_lists = BetterTaskList.select().execute()
    #tasks = Task.select().execute()
    #pprint(task_lists.dump())
    #pprint(tasks.dump())
    #pprint(task_lists.load('tasks').dump())
    pprint(task_lists.final_task.dump())
