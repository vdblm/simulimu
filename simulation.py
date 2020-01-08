from collections import defaultdict


class TimeServer:
    def __init__(self, rate):
        """

        :param rate: poisson dist rate
        """
        pass

    def add_task(self, task):
        """

        :type task: Task
        :return:
        """


class ProcessServer:
    def __init__(self):
        pass


class Task:
    def __init__(self):
        # TODO complete Task class
        """
        parameters
        -------------
        type: it can be 1 or 2
        """
        pass


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
                self.next_coming_task = self.time + self.generate_inter_arrival()

                task_id = self.last_id + 1
                self.last_id = task_id

                deadline = self.time + self.generate_deadline()
                self.tasks_deadline[deadline].append(task_id)

                task = Task()
                self.tasks[task_id] = task
                self.time_server.add_task(task)

            # TODO check for deadlines
            dead_tasks = self.tasks_deadline.get(self.time)
            self.handle_dead_tasks(dead_tasks)

            self.time += 1

    def generate_inter_arrival(self):
        pass
