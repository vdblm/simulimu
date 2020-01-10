from collections import defaultdict


class Queue:
    def __init__(self):
        self.queue1 = []
        self.queue2 = []

    def add_item(self, task):
        """

        :type task: Task
        """
        if task.type is 1:
            self.queue1.append(task)
        elif task.type is 2:
            self.queue2.append(task)
        else:
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


class TimeServer:
    def init(self, rate, process_servers):
        self.rate = rate
        self.process_servers = process_servers
        self.queue = Queue()
        """

        :param rate: poisson dist rate
        """
        pass

    def add_task(self, task):
        if self.queue.is_empty():
            length = [i.queue.length() for i in self.process_servers]
            self.process_servers[length.index(min(length))].add_task(task)
        else:
            self.queue.add_item(task)
        """
        :type task: Task
        :return:
        """


class ProcessServer:
    def init(self, number_of_processor, averages):
        self.queue = Queue()
        self.number_of_processor = number_of_processor
        self.averages = averages
        self.status = [False for i in range(self.number_of_processor)]

    def add_task(self, task):
        flag = False
        for i in range(self.number_of_processor):
            if not self.status[i] and not flag:
                self.status[i] = task
                flag = True
        if not flag:
            self.queue.add_item(task)

    def done_task(self, task):
        for i in range(self.number_of_processor):
            if self.status[i] == task:
                task.done()
                if self.queue.is_empty():
                    self.status[i] = False
                else:
                    self.status[i] = self.queue.remove()


class Task:
    def __init__(self, type, arrival_time):
        # TODO complete Task class
        """
        parameters
        -------------
        type: it can be 1 or 2
        """
        self.type = type
        self.deadline_passed = False
        self.is_done = False
        self.arrival_time = arrival_time
        self.start_time = None
        self.service_time = None
        self.server = None

    def done(self):
        self.is_done = True


class Simulation:
    def __init__(self):
        """
        tasks_deadline = {deadline_time: [task ids]}
        tasks_done_time = {done_time: [task ids]}
        tasks = {task_id: Task object}
        """
        self.tasks_deadline = defaultdict(list)
        self.tasks_done_time = defaultdict(list)
        self.tasks_start_time = defaultdict(list)
        self.tasks = {}

        self.finished = False
        self.time = 0

        self.time_server = None
        """
        process_servers = [ProcessServer objects]
        """
        self.process_servers = []

        self.last_id = -1
        self.next_coming_task = 0

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

    def run_simulation(self):
        """
        TODO: when to generate arrival times?
        we can sample an exponential r.v. as the inter-arrival when a new task comes
        """
        while not self.finished:
            if self.time is self.next_coming_task:
                # TODO add a new task
                dt, type = self.generate_inter_arrival()
                self.next_coming_task = self.time + dt

                task_id = self.last_id + 1
                self.last_id = task_id

                deadline = self.time + self.generate_deadline()
                self.tasks_deadline[deadline].append(task_id)

                task = Task(type, self.time)
                self.tasks[task_id] = task
                self.time_server.add_task(task)

            # TODO check for deadlines
            dead_tasks = self.tasks_deadline.get(self.time)
            self.handle_dead_tasks(dead_tasks)

            # TODO check for done tasks
            self.handle_done_tasks(self.tasks_done_time.get(self.time))

            self.time += 1

    def generate_inter_arrival(self):
        pass

    def generate_deadline(self):
        pass

    def handle_dead_tasks(self, dead_tasks):
        pass

    def handle_done_tasks(self, param):
        pass
