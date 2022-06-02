import json

import requests
import argparse
import time
from multiprocessing import Process, Queue
from crypto_tools.stats import NgramCounter


def download_and_count_books(num, q_in, q_out, nvalue):
    while not q_in.empty():
        book = q_in.get()
        print("Requesting {}: {}".format(book['book'], book['title']))
        try:
            r = requests.get(book['book'])
            if r.status_code == 200:
                c = NgramCounter(text=r.text[book['start']:book['end']], nvalue=nvalue).count()
                print("Sending {} to queue.".format(book['title']))
            else:
                print("{}: ERROR {}".format(book['title'], r.status_code))
            q_out.put(c)
        except Exception as e:
            print("Could not load {}".format(book['title']))
            print(e)
    print("Process {} finished!".format(num))


def collapse_counts(q_in):
    output = {}
    total = 0
    try:
        while True:
            book_count = q_in.get(block=True, timeout=20)
            for ngram in book_count.keys():
                total += book_count[ngram]
                if ngram in output.keys():
                    output[ngram] += book_count[ngram]
                else:
                    output[ngram] = book_count[ngram]
            print(".")
    except:
        print("Ngram queue empty")
    return output, total


def calculate_ngram_frequency(ngram_count, total):
    output = {}
    for ngram in ngram_count.keys():
        output[ngram] = ngram_count[ngram] / total
    return output


def write_output(outfile_name, ngram_frequency):
    with open(outfile_name, "w") as outfile:
        for ngram in ngram_frequency.keys():
            outfile.write("'{ngram}': {frequency:.15f},\n".format(ngram=ngram, frequency=ngram_frequency[ngram]))


def read_books(input_filename):
    with open(input_filename, "r") as infile:
        book_list = json.load(infile)
    return book_list


if __name__ == "__main__":
    time_start = int(time.time())
    default_outfile_name = "out_gutenberg_{}.txt".format(str(int(time.time())))
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("-p", "--processes", type=int, default=1,
                                 help="Number of max processes (default 1).")
    argument_parser.add_argument("-o", "--out", type=str, default=default_outfile_name,
                                 help="Optional name of the output file.")
    argument_parser.add_argument("-n", "--nvalue", type=int, default=4,
                                 help="Size of each ngram")
    argument_parser.add_argument("-l", "--list", type=str, default="book_list.json",
                                 help="The name of the file that contains the list of books to be used.")
    args = argument_parser.parse_args()
    outfile_name = args.out
    books = read_books(args.list)
    processes = []
    books_queue = Queue()
    ngrams_queue = Queue()
    print("Queueing books...")
    for book in books:
        books_queue.put(book)
    print("Spawning downloaders...")
    for i in range(args.processes):  # spawn the processes
        proc = Process(target=download_and_count_books, args=(i, books_queue, ngrams_queue, args.nvalue))
        processes.append(proc)
        proc.start()
    print("Merging the counts...")
    ngram_count, total_ngrams = collapse_counts(ngrams_queue)
    ngram_frequency = calculate_ngram_frequency(ngram_count, total_ngrams)
    print("Closing downloaders...")
    for proc in processes:  # close the processes
        proc.join()
    print("Writing results...")
    write_output(outfile_name, ngram_frequency)
    print("Total time: " + str(int(time.time()) - time_start))
