import json

input_nodes = []
output_nodes = []


class OutputNode:
    def __init__(self, task_id, released_time, deadline, execution_times):
        self.task_id = task_id
        self.released_time = released_time
        self.deadline = deadline
        self.execution_times = execution_times


def read_nodes_from_input(file):
    global input_nodes
    json_file = open(file)
    input_nodes = json.load(json_file)


def initialize_tasks():
    for node in input_nodes:
        task_id = node['taskId']
        task_new_released_time = find_new_release_time(task_id)
        task_new_deadline = find_new_deadline(task_id)
        new_output_node = OutputNode(task_id, task_new_released_time, task_new_deadline, [-1, -1])
        output_nodes.append(new_output_node)


def find_new_release_time(task_id):
    task = find_task_by_id(task_id)
    task_parent_id = task['parentId']
    task_parent = find_task_by_id(task_parent_id)
    if task_parent is None:
        return task['releasedTime']
    return max(task['releasedTime'], find_new_release_time(task_parent_id) + task_parent['executionTime'])


def find_task_by_id(task_id):
    if task_id is None:
        return None
    for task in input_nodes:
        if task['taskId'] == task_id:
            return task
    return None


def find_new_deadline(task_id):
    task = find_task_by_id(task_id)
    task_children = find_task_children(task_id)
    task_new_deadline = task['deadline']
    for task_child_id in task_children:
        task_child = find_task_by_id(task_child_id)
        task_new_deadline = min(task_new_deadline, find_new_deadline(task_child_id) - task_child['executionTime'])
    return task_new_deadline


def find_task_children(task_id):
    task_children_ids = []
    for node in input_nodes:
        if node['parentId'] == task_id:
            task_children_ids.append(node['taskId'])
    return task_children_ids


def order_tasks():
    time = -1
    release_times = get_release_times()
    tasks_and_their_executed_times = {}
    executing_task_id = None
    tasks_in_queue = []
    num_of_finished_tasks = 0
    num_of_tasks = len(output_nodes)

    while True:
        if num_of_finished_tasks == num_of_tasks:
            break
        time = time + 1
        update_tasks_in_queue_based_on_release_times(release_times, tasks_in_queue, time)

        if executing_task_id is None:
            if len(tasks_in_queue) == 0:
                continue
            executing_task_id = choose_next_executing_task(tasks_and_their_executed_times, tasks_in_queue, time)

        else:
            increment_executing_task_execution_time(executing_task_id, tasks_and_their_executed_times)
            if is_task_finished(executing_task_id, tasks_and_their_executed_times):
                set_task_execution_time(executing_task_id, time)
                tasks_in_queue.remove(find_output_task_by_id(executing_task_id))
                num_of_finished_tasks = num_of_finished_tasks + 1
                executing_task_id = None
            if len(tasks_in_queue) == 0:
                continue
            executing_task_id = choose_next_executing_task(tasks_and_their_executed_times, tasks_in_queue, time)


def get_release_times():
    release_times = []
    for node in output_nodes:
        release_times.append(node.released_time)
    release_times.sort(reverse=True)
    return release_times


def update_tasks_in_queue_based_on_release_times(release_times, tasks_in_queue, time):
    if len(release_times) > 0 and time == release_times[-1]:
        while len(release_times) > 0 and time == release_times[-1]:
            release_times.pop()
        tasks_in_queue.extend(get_released_tasks_by_release_time(time))


def get_released_tasks_by_release_time(time):
    released_tasks = []
    for task in output_nodes:
        if task.released_time == time:
            released_tasks.append(task)
    return released_tasks


def choose_next_executing_task(tasks_and_their_executed_times, tasks_in_queue, time):
    next_executing_task_id = choose_executing_task(tasks_in_queue)
    if next_executing_task_id not in tasks_and_their_executed_times:
        tasks_and_their_executed_times[next_executing_task_id] = 0
        executing_task = find_output_task_by_id(next_executing_task_id)
        executing_task.execution_times[0] = time
    return next_executing_task_id


def choose_executing_task(queue):
    executing_task = queue[0]
    for task in queue:
        if task.deadline < executing_task.deadline:
            executing_task = task
    return executing_task.task_id


def find_output_task_by_id(task_id):
    for task in output_nodes:
        if task.task_id == task_id:
            return task
    return None


def increment_executing_task_execution_time(executing_task_id, tasks_and_their_executed_times):
    tasks_and_their_executed_times[executing_task_id] = tasks_and_their_executed_times[executing_task_id] + 1


def is_task_finished(executing_task_id, tasks_and_their_executed_times):
    return tasks_and_their_executed_times[executing_task_id] == find_task_by_id(executing_task_id)['executionTime']


def set_task_execution_time(executing_task_id, time):
    find_output_task_by_id(executing_task_id).execution_times[1] = time


def write_nodes_to_output(file):
    json_output = json.dumps(output_nodes, default=vars, indent=2)
    with open(file, 'w') as outfile:
        outfile.write(json_output)


read_nodes_from_input('Input.json')
initialize_tasks()
order_tasks()
write_nodes_to_output('Output.json')
