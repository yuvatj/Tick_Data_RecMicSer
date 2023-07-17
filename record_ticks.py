import multiprocessing
from kite_websocket import TickData

tick = TickData()


def record_nse():
    tick.record('NSE')


def record_nfo():
    tick.record('NFO')


if __name__ == '__main__':
    process1 = multiprocessing.Process(target=record_nse)
    process2 = multiprocessing.Process(target=record_nfo)

    process1.start()
    process2.start()

    process1.join()
    process2.join()
