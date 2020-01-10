from collections import defaultdict
import numpy as np


class Queue:
    def __init__(self):
        self.queue1 = []
        self.queue2 = []

    def add_item(self, task):
        """

        :type task: Task
        """
        if task.type == 1:
            self.queue1.append(task)
        elif task.type == 2:
            self.queue2.append(task)
        else:
            print(task.type)
            raise Exception('Task type undefined')

    def remove(self):
        if len(self.queue1) > 0:
            return self.queue1.pop(0)
        else:
            return self.queue2.pop(0)

    def is_empty(self):
        if len(self.queue2) is 0 and len(self.queue1) is 0:
            return True

    def length(self):
        return len(self.queue1) + len(self.queue2)

    def remove_task(self, task):
        if task in self.queue1:
            self.queue1.remove(task)
        elif task in self.queue2:
            self.queue2.remove(task)


class TimeServer:
    def __init__(self, rate):
        self.rate = rate
        self.process_servers = []
        self.queue = Queue()
        self.status = False
        self.done_time = False
        """

        :param rate: poisson dist rate 
        """
        pass

    def add_process_server(self, process_server):
        self.process_servers.append(process_server)

    def __start_task(self, task):
        service_time = generate_service_time(self.rate)
        # set the task start time and service time
        self.done_time = task.simulation.time + service_time
        return task

    def add_task(self, task):
        if self.queue.is_empty() and self.status is False:
            self.status = self.__start_task(task)
        else:
            self.queue.add_item(task)
        """
        :type task: Task
        :return:
        """

    def done_task(self):
        task = self.status
        if self.queue.is_empty():
            self.status = False
        else:
            self.status = self.__start_task(task=self.queue.remove())

        length = [i.queue.length() for i in self.process_servers]
        server_num = np.random.choice(np.where(length == np.min(length))[0])
        server = self.process_servers[server_num]
        server.add_task(task)
        task.server = server

    def task_dead(self, task):
        if self.status is task:
            if self.queue.is_empty():
                self.status = False
            else:
                self.status = self.__start_task(task=self.queue.remove())
        else:
            self.queue.remove_task(task)


def generate_service_time(param):
    # TODO different for timeserver and processerver
    return 2


class ProcessServer:
    def __init__(self, number_of_processor, averages):
        self.queue = Queue()
        self.number_of_processor = number_of_processor
        self.averages = averages
        self.status = [False for i in range(self.number_of_processor)]

    def add_task(self, task):
        flag = False
        for i in range(self.number_of_processor):
            if self.queue.is_empty() and self.status[i] is False and not flag:
                self.status[i] = self.__start_task(task, i)
                flag = True
        if not flag:
            self.queue.add_item(task)

    def __start_task(self, task, core_number):
        # set the task start time and service time
        service_time = generate_service_time(self.averages[core_number])
        task.start_service(service_time)
        return task

    def done_task(self, task):
        for i in range(self.number_of_processor):
            if self.status[i] == task:
                if self.queue.is_empty():
                    self.status[i] = False
                else:
                    self.status[i] = self.__start_task(task=self.queue.remove(), core_number=i)

    def task_dead(self, task):
        self.queue.remove_task(task)


class Task:
    def __init__(self, type, arrival_time, deadline, simulation, server):
        """
        parameters
        -------------
        simulation: Simulation class
        type: it can be 1 or 2
        """
        self.type = type
        self.deadline = deadline
        self.deadline_passed = False
        self.is_done = False
        self.arrival_time = arrival_time
        self.start_time = None
        self.service_time = None
        self.server = server
        self.simulation = simulation

    def done(self):
        self.is_done = True
        self.server.done_task(self)

    def start_service(self, service_time):
        self.service_time = service_time
        self.start_time = self.simulation.time
        # add task to tasks_done_time dictionary in simulation
        self.simulation.tasks_done_time[self.start_time + self.service_time].append(self)

    def dead(self):
        if self.start_time is None:
            self.deadline_passed = True
        self.server.task_dead(self)


class Simulation:
    def __init__(self, new_task_rate, deadline_avg):
        """
        tasks_deadline = {deadline_time: [task]}
        tasks_done_time = {done_time: [task]}
        tasks = {task_id: Task object}
        """
        self.tasks_deadline = defaultdict(list)
        self.tasks_done_time = defaultdict(list)
        self.tasks = []

        self.new_task_rate = new_task_rate
        self.deadline_avg = deadline_avg

        # TODO when to finish?
        self.finished = False
        self.time = 0

        self.time_server = None
        """
        process_servers = [ProcessServer objects]
        """
        self.process_servers = []

        self.next_coming_task = 0
        self.next_type = 2

    def set_time_server(self, time_server):
        """

        :type time_server: TimeServer
        """
        self.time_server = time_server

    def add_process_server(self, process_server):
        """

        :type process_server: ProcessServer
        """
        self.process_servers.append(process_server)

    def add_new_task(self):
        type = self.next_type

        dt, self.next_type = self.generate_inter_arrival()
        self.next_coming_task = self.time + dt

        deadline = self.time + self.generate_deadline()
        task = Task(type, self.time, deadline, self, self.time_server)
        self.tasks_deadline[deadline].append(task)

        self.tasks.append(task)
        self.time_server.add_task(task)

    def run_simulation(self):
        """
        we can sample an exponential r.v. as the inter-arrival when a new task comes
        """
        while not self.finished:
            if self.time is self.next_coming_task:
                # add a new task
                self.add_new_task()

            # check for deadlines
            if self.time in self.tasks_deadline:
                dead_tasks = self.tasks_deadline.pop(self.time)
                self.handle_dead_tasks(dead_tasks)

            # check for done tasks
            if self.time_server.done_time is self.time:
                self.time_server.done_task()
            if self.time in self.tasks_done_time:
                done_tasks = self.tasks_done_time.pop(self.time)
                self.handle_done_tasks(done_tasks)

            # TODO correct it
            if self.time is 8:
                self.finished = True

            self.time += 1

    def generate_inter_arrival(self):
        # TODO based on self.new_task_rate
        # TODO make it integer!!
        return 1, np.random.choice([1, 2])

    def generate_deadline(self):
        # TODO should be done based on self.deadline_avg
        return 4

    def handle_dead_tasks(self, dead_tasks):
        for dead_task in dead_tasks:
            dead_task.dead()

    def handle_done_tasks(self, done_tasks):
        for done_task in done_tasks:
            done_task.done()


if __name__ == '__main__':
    inp = input('Enter M, lambda, alpha, mu\n').split(', ')
    m = int(inp[0])
    l = float(inp[1])
    alpha = float(inp[2])
    mu = float(inp[3])
    simulation = Simulation(new_task_rate=l, deadline_avg=alpha)
    time_server = TimeServer(rate=mu)
    simulation.time_server = time_server
    for i in range(m):
        inp = input('Enter N, mu_1, ..., mu_j\n').split(', ')
        n = int(inp[0])

        mu_s = [float(inp[j + 1]) for j in range(n)]
        process_server = ProcessServer(n, mu_s)
        simulation.add_process_server(process_server)
        time_server.add_process_server(process_server)

    simulation.run_simulation()
    print('hi')
