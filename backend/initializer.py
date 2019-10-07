# import multiprocessing
import rx
from rx import operators as ops

from multiprocessing import Process
from concurrent.futures import ProcessPoolExecutor


# def process_initializer(iterable, consumer):
def worker(consumer):
    # num_cores = multiprocessing.cpu_count()
    with ProcessPoolExecutor(3) as executor:
        rx.from_(consumer.iterable).pipe(
            ops.map(
                lambda msg: executor.submit(consumer.process_consumer, msg)
            ),
        ).subscribe(consumer)


class RXInitializer(object):

    def __init__(self, consumers):
        self.processes = self.__get_processes(consumers)
        print("Get process: ", self.processes)

    def __get_processes(self, consumers):
        return [
            Process(target=worker, args=(consumer,))
            for consumer in consumers
        ]

    def run(self):
        [
            process.start()
            for process in self.processes
        ]

    def release(self):
        return [
            process.join()
            for process in self.processes
        ]
