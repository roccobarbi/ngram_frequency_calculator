import requests
import argparse
import time
from multiprocessing import Process, Queue
from crypto_tools.stats import NgramCounter


def download_and_count_books(num, q_in, q_out, nvalue):
    while not q_in.empty():
        book = q_in.get()
        print("Requesting {}".format(book))
        try:
            r = requests.get(book)
            if r.status_code == 200:
                c = NgramCounter(text=r.text[1000:-18946], nvalue=nvalue).count()
                print("Sending {} to queue.".format(book))
            else:
                print("{} ERROR {}".format(book, r.status_code))
            q_out.put(c)
        except Exception as e:
            print("Could not load {}".format(book))
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
    args = argument_parser.parse_args()
    outfile_name = args.out
    books = [
        "https://gutenberg.org/ebooks/514.txt.utf-8",
        "https://gutenberg.org/files/10/10-0.txt",
        "https://gutenberg.org/files/11/11-0.txt",
        "https://gutenberg.org/files/12/12-0.txt",
        "https://gutenberg.org/files/13/13-0.txt",
        "https://gutenberg.org/files/15/15-0.txt",
        "https://gutenberg.org/files/16/16-0.txt",
        "https://gutenberg.org/files/17/17-0.txt",
        "https://gutenberg.org/files/18/18-0.txt",
        "https://gutenberg.org/files/19/19-0.txt",
        "https://gutenberg.org/files/20/20-0.txt",
        "https://gutenberg.org/files/21/21-0.txt",
        "https://gutenberg.org/files/22/22-0.txt",
        "https://gutenberg.org/files/23/23-0.txt",
        "https://gutenberg.org/files/24/24-0.txt",
        "https://gutenberg.org/files/26/26-0.txt",
        "https://gutenberg.org/files/27/27-0.txt",
        "https://gutenberg.org/files/28/28-0.txt",
        "https://gutenberg.org/files/30/30-0.txt",
        "https://gutenberg.org/files/31/31-0.txt",
        "https://gutenberg.org/files/32/32-0.txt",
        "https://gutenberg.org/files/33/33-0.txt",
        "https://gutenberg.org/files/35/35-0.txt",
        "https://gutenberg.org/files/36/36-0.txt",
        "https://gutenberg.org/files/41/41-0.txt",
        "https://gutenberg.org/files/42/42-0.txt",
        "https://gutenberg.org/files/43/43-0.txt",
        "https://gutenberg.org/files/44/44-0.txt",
        "https://gutenberg.org/files/45/45-0.txt",
        "https://gutenberg.org/files/46/46-0.txt",
        "https://gutenberg.org/files/47/47-0.txt",
        "https://gutenberg.org/files/76/76-0.txt",
        "https://gutenberg.org/files/82/82-0.txt",
        "https://gutenberg.org/files/84/84-0.txt",
        "https://gutenberg.org/files/98/98-0.txt",
        "https://gutenberg.org/files/120/120-0.txt",
        "https://gutenberg.org/files/174/174-0.txt",
        "https://gutenberg.org/files/219/219-0.txt",
        "https://gutenberg.org/files/345/345-0.txt",
        "https://gutenberg.org/files/408/408-0.txt",
        "https://gutenberg.org/files/421/421-0.txt",
        "https://gutenberg.org/files/647/647-0.txt",
        "https://gutenberg.org/files/844/844-0.txt",
        "https://gutenberg.org/files/848/848-0.txt",
        "https://gutenberg.org/files/1080/1080-0.txt",
        "https://gutenberg.org/files/1342/1342-0.txt",
        "https://gutenberg.org/files/1661/1661-0.txt",
        "https://gutenberg.org/files/1952/1952-0.txt",
        "https://gutenberg.org/files/2542/2542-0.txt",
        "https://gutenberg.org/files/2591/2591-0.txt",
        "https://gutenberg.org/files/2701/2701-0.txt",
        "https://gutenberg.org/files/4300/4300-0.txt",
        "https://gutenberg.org/files/5200/5200-0.txt",
        "https://gutenberg.org/files/45506/45506-0.txt",
        "https://gutenberg.org/files/51954/51954-0.txt",
        "https://gutenberg.org/files/64131/64131-0.txt",
        "https://gutenberg.org/cache/epub/14506/pg14506.txt",
        "https://gutenberg.org/cache/epub/15660/pg15660.txt",
        "https://gutenberg.org/cache/epub/18665/pg18665.txt",
        "https://gutenberg.org/cache/epub/20213/pg20213.txt",
        "https://gutenberg.org/cache/epub/22677/pg22677.txt",
        "https://gutenberg.org/cache/epub/25344/pg25344.txt",
        "https://gutenberg.org/cache/epub/26399/pg26399.txt",
        "https://gutenberg.org/cache/epub/34524/pg34524.txt",
        "https://gutenberg.org/cache/epub/34669/pg34669.txt",
        "https://gutenberg.org/cache/epub/34670/pg34670.txt",
        "https://gutenberg.org/cache/epub/34671/pg34671.txt",
        "https://gutenberg.org/cache/epub/34672/pg34672.txt",
        "https://gutenberg.org/cache/epub/34673/pg34673.txt",
        "https://gutenberg.org/cache/epub/34829/pg34829.txt",
        "https://gutenberg.org/cache/epub/43025/pg43025.txt",
        "https://gutenberg.org/cache/epub/48990/pg48990.txt",
        "https://gutenberg.org/cache/epub/49330/pg49330.txt",
        "https://gutenberg.org/cache/epub/58617/pg58617.txt",
        "https://gutenberg.org/cache/epub/60912/pg60912.txt",
        "https://gutenberg.org/cache/epub/61217/pg61217.txt",
        "https://gutenberg.org/cache/epub/64317/pg64317.txt",
        "https://gutenberg.org/cache/epub/67190/pg67190.txt",
        "https://gutenberg.org/cache/epub/67191/pg67191.txt",
        "https://gutenberg.org/cache/epub/67195/pg67195.txt"
    ]
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
