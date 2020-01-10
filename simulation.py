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
    def __init__(self, rate, process_servers):
        self.rate = rate
        self.process_servers = process_servers
        self.queue = Queue()
        self.status = False
        """

        :param rate: poisson dist rate
        """
        pass

    def __start_task(self, task):
        service_time = generate_service_time(self.rate)
        task.start_service(service_time)
        return task

    def add_task(self, task):
        if self.queue.is_empty():
            self.status = self.__start_task(task)
        else:
            self.queue.add_item(task)
        """
        :type task: Task
        :return:
        """

    def done_task(self, task):
        if self.queue.is_empty():
            self.status = False
        else:
            self.status = self.__start_task(task=self.queue.remove())

        length = [i.queue.length() for i in self.process_servers]
        server = self.process_servers[length.index(min(length))]
        server.add_task(task)
        task.server = server


def generate_service_time(param):
    pass


class ProcessServer:
    def __init__(self, number_of_processor, averages):
        self.queue = Queue()
        self.number_of_processor = number_of_processor
        self.averages = averages
        self.status = [False for i in range(self.number_of_processor)]

    def add_task(self, task):
        flag = False
        for i in range(self.number_of_processor):
            if self.status[i] is False and not flag:
                self.status[i] = self.__start_task(task, i)
                flag = True
        if not flag:
            self.queue.add_item(task)

    def __start_task(self, task, core_number):
        service_time = generate_service_time(self.averages[core_number])
        task.start_service(service_time)
        return task

    def done_task(self, task):
        for i in range(self.number_of_processor):
            if self.status[i] == task:
                task.done()
                if self.queue.is_empty():
                    self.status[i] = False
                else:
                    self.status[i] = self.__start_task(task=self.queue.remove(), core_number=i)


class Task:
    def __init__(self, type, arrival_time, deadline):
        # TODO complete Task class
        """
        parameters
        -------------
        type: it can be 1 or 2
        """
        self.type = type
        self.deadline = deadline
        self.deadline_passed = False
        self.is_done = False
        self.arrival_time = arrival_time
        self.start_time = None
        self.service_time = None
        self.server = None

    def done(self):
        self.is_done = True

    def start_service(self, service_time):
        self.service_time = service_time
        # TODO add task to tasks_done_time dictionary in simulation


class Simulation:
    def __init__(self):
        """
        tasks_deadline = {deadline_time: [task]}
        tasks_done_time = {done_time: [task]}
        tasks = {task_id: Task object}
        """
        self.tasks_deadline = defaultdict(list)
        self.tasks_done_time = defaultdict(list)
        self.tasks_start_time = defaultdict(list)
        self.tasks = []

        self.finished = False
        self.time = 0

        self.time_server = None
        """
        process_servers = [ProcessServer objects]
        """
        self.process_servers = []

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

                deadline = self.time + self.generate_deadline()
                task = Task(type, self.time, deadline)
                self.tasks_deadline[deadline].append(task)

                self.tasks.append(task)
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
