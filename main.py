import json

output = []


class OutputNode:
    def __init__(self, task_id, released_time, deadline, execution_times):
        self.task_id = task_id
        self.released_time = released_time
        self.deadline = deadline
        self.execution_times = execution_times


def read_nodes_from_json(file):
    return json.load(file)


def find_task_by_id(task_id):
    if task_id is None:
        return None
    for i in range(0, len(input_nodes)):
        task = input_nodes[i]
        if task['taskId'] == task_id:
            return task
    return None


def find_new_release_time(task_id):
    task = find_task_by_id(task_id)
    task_parent_id = task['parentId']
    task_parent = find_task_by_id(task_parent_id)
    if task_parent is None:
        return task['releasedTime']
    return max(task['releasedTime'], find_new_release_time(task_parent_id) + task_parent['executionTime'])


def find_task_children(task_id):
    task_children_ids = []
    for i in range(0,len(input_nodes)):
        node = input_nodes[i]
        if node['parentId'] == task_id:
            task_children_ids.append(node['taskId'])
    return task_children_ids


def find_new_deadline(task_id):
    task = find_task_by_id(task_id)
    task_children = find_task_children(task_id)
    task_new_deadline = task['deadline']
    for i in range(0, len(task_children)):
        task_child_id = task_children[i]
        task_child = find_task_by_id(task_child_id)
        task_new_deadline = min(task_new_deadline, find_new_deadline(task_child_id) - task_child['executionTime'])
    return task_new_deadline


def initialize_tasks():
    for i in range(0, len(input_nodes)):
        node = input_nodes[i]
        task_id = node['taskId']
        task_new_released_time = find_new_release_time(task_id)
        task_new_deadline = find_new_deadline(task_id)
        new_output_node = OutputNode(task_id, task_new_released_time, task_new_deadline, [-1, -1])
        output.append(new_output_node)


def find_tasks_in_queue(time):
    tasks_in_queue = []
    for i in range(0, len(output)):
        if output[i].released_time <= time and (output[i].execution_times[1] == -1):
            tasks_in_queue.append(output[i])
    return tasks_in_queue


def find_output_task_by_id(id):
    for i in range (len(output)):
        if output[i].task_id == id:
            return output[i]
    return None


def choose_executing_task(queue):
    task = queue[0]
    for i in range (1, len(queue)):
        if queue[i].deadline < task.deadline:
            task = queue[i]
    return task.task_id


def order_tasks():
    time = -1
    tasks_and_their_executed_times = {}
    executing_task_id = None
    executing_time_of_executing_task = 0
    tasks_in_queue = []
    num_of_finished_tasks = 0
    num_of_tasks = len(output)
    while True:
        time = time + 1
        if executing_task_id is not None:
            executing_time_of_executing_task = executing_time_of_executing_task + 1
            tasks_and_their_executed_times[executing_task_id] = executing_time_of_executing_task
            if executing_time_of_executing_task == find_task_by_id(executing_task_id)['executionTime']:
                find_output_task_by_id(executing_task_id).execution_times[1] = time
                num_of_finished_tasks = num_of_finished_tasks + 1
                executing_task_id = None
                executing_time_of_executing_task = 0
            tasks_in_queue = find_tasks_in_queue(time)
            if len(tasks_in_queue) == 0: # Not all tasks are executed but now there is no task to execute
                continue
            if choose_executing_task(tasks_in_queue) != executing_task_id:
                executing_task_id = choose_executing_task(tasks_in_queue)
                if executing_task_id in tasks_and_their_executed_times:  # Task has been preempted before
                    executing_time_of_executing_task = tasks_and_their_executed_times.get(executing_task_id)
                else:  # It's the first time this task is being dispatched
                    executing_time_of_executing_task = 0
                    tasks_and_their_executed_times[executing_task_id] = 0
                    find_output_task_by_id(executing_task_id).execution_times[0] = time

        else:
            if num_of_finished_tasks == num_of_tasks: # All tasks are executed
                break
            tasks_in_queue = find_tasks_in_queue(time)
            if len(tasks_in_queue) == 0: # Not all tasks are executed but now there is no task to execute
                continue
            executing_task_id = choose_executing_task(tasks_in_queue)
            if executing_task_id in tasks_and_their_executed_times: # Task has been preempted before
                executing_time_of_executing_task = tasks_and_their_executed_times.get(executing_task_id)
            else: # It's the first time this task is being dispatched
                executing_time_of_executing_task = 0
                tasks_and_their_executed_times[executing_task_id] = 0
                find_output_task_by_id(executing_task_id).execution_times[0] = time


json_file = open('Input.json')
input_nodes = read_nodes_from_json(json_file)
initialize_tasks()
order_tasks()
json_output = json.dumps(output, default=vars)
with open('Output.json', 'w') as outfile:
    outfile.write(json_output)
