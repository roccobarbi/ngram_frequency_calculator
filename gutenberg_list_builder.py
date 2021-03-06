import json
import requests
import argparse
import time

BASE_URL = "https://gutenberg.org/files/{}/{}-0.txt"

def main():
    default_outfile_name = "list_gutenberg_files_{}.txt".format(str(int(time.time())))
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("-o", "--out", type=str, default=default_outfile_name,
                                 help="Optional name of the output file.")
    args = argument_parser.parse_args()
    unknown = 0
    count = 0
    books = []
    while unknown < 100:
        book = BASE_URL.format(count, count)
        try:
            if requests.head(book).status_code == 200:
                text = requests.get(book).text
                start = text.index("*** START OF THE PROJECT GUTENBERG EBOOK") + 40
                start += text[start:].index("***") + 3
                end = text.index("*** END")
                language_s = text.index("Language:") + 9
                language_e = text.index("\n", language_s)
                title_s = text.index("Title:") + 6
                title_e = text.index("\n", title_s)
                book_descriptor = {
                    "book": book,
                    "start": start,
                    "end": end,
                    "language": text[language_s:language_e].strip(),
                    "title": text[title_s:title_e].strip()
                }
                books.append(book_descriptor)
                unknown = 0
            else:
                unknown += 1
        except:
            unknown += 1
        count += 1
        print("{:>10}: {:0>3}".format(count, unknown))
    with open(args.out, "w") as outfile:
        outfile.write(json.dumps(books))


if __name__ == "__main__":
    main()
