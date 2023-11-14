
import time
import datetime
import multiprocessing

print("Recording preprocessing started")

# Get the current time.
time_live = datetime.datetime.now().time().replace(microsecond=0)

start_time = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 00))

# Calculate the delay until the start time
if time_live < start_time.time():
    time_diff = (start_time - datetime.datetime.today()).seconds
    print(f"Waiting {time_diff} seconds.")
    time.sleep(time_diff + 1)

from kite_websocket import TickData

tick = TickData()


def record_nse():
    tick.record_beta('NSE')


def record_nfo():
    tick.record_beta('NFO')


def record_index():
    tick.record_beta('INDEX')


if __name__ == '__main__':
    print("Tick data recording started")

    # Get the current time.
    time_live = datetime.datetime.now().time().replace(microsecond=0)

    start_time = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 14))

    # Calculate the delay until the start time
    if time_live < start_time.time():
        time_diff = (start_time - datetime.datetime.today()).seconds
        print(f"Waiting for {time_diff} seconds.")
        time.sleep(time_diff + 1)

    process1 = multiprocessing.Process(target=record_nse)
    process2 = multiprocessing.Process(target=record_nfo)
    process3 = multiprocessing.Process(target=record_index)

    process1.start()
    process2.start()
    process3.start()

    process1.join()
    process2.join()
    process3.join()
