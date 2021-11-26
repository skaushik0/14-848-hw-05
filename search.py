#! /usr/bin/env python3

'''
search.py:  Search for a word or top 'N' terms in a given
            set of documents by building an inverted index.
'''

import os
import sys
import tarfile
import tempfile
from pyspark import SparkContext # pylint: disable=import-error

# As specified in the write-up, the following words are "stop words"
# and should be excluded with the word filter.
STOP_WORDS = ["they", "she", "he", "it", "the", "as", "is", "and"]

def add_files(doc_dir, tmp_dir, spark_ctx):
    '''
    Add files to context.
    '''
    rdd = None

    # First, extract all the files.
    for doc_gz in os.listdir(doc_dir):
        path = os.path.join(doc_dir, doc_gz)
        if os.path.isfile(path):
            dst = os.path.join(
                tmp_dir,
                path.split('/')[-1].split('.')[0]
            )
            tar_file = tarfile.open(path, "r:gz")
            tar_file.extractall(dst)

    # Then, build a list of RDDs.
    rdds = []
    for root, _, files in os.walk(tmp_dir):
        for doc_ex in files:
            rdds.append(spark_ctx.wholeTextFiles(os.path.join(root, doc_ex)))

    # Create a union of the RDDs and add it to the context.
    rdd = spark_ctx.union(rdds)

    return rdd

def tokenize_flip(rdd):
    '''
    Tokenize the text in the file and flip the key-value pair in the
    flat-map.

    The "wholeTextFiles" function returns tuples of ("path", "text"),
    where "path" is the file-path and "text" is the file content. Here,
    we read the content, tokenize it and flip the key of the flat-map
    to be the word, and the file-path to be the value. This makes it
    easy for us when we're counting words.
    '''
    return rdd.flatMap(lambda path_text: [
        (word, path_text[0]) for word in path_text[1].lower().split()
    ])

def filter_stop_words(words):
    '''
    Filter out stop-words as specified in the handout.
    '''
    return words.filter(lambda word_path: word_path[0] not in STOP_WORDS)

def init_word_count(words):
    '''
    Initialize all words counts to be 1.

    This is the pre-processing step before we call the reducer.
    With all the word frequencies set to 1, we can then reduce
    by summing up the individual word counts and aggregating the
    results.

    The tuple format is (("word", "file-path"), "count"). Note that
    we're using the ("word", "file-path") tuple as a single key when
    calling the reducer. It should be this way because we want to get
    the count of individual word occurances in a given file.
    '''
    return words.map(lambda word_path: ((word_path[0], word_path[1]), 1))

def reduce_word_count(words):
    '''
    Aggregate the word counts by ("word", "file-path") as the key.
    '''
    return words.reduceByKey(lambda x, y: x + y)

def build_index(words):
    '''
    Build an index (a "flat" version of the reducer output to
    search for terms).
    '''
    return words.map(lambda word_path_count: (
        word_path_count[0][0], (word_path_count[0][1], word_path_count[1])
    ))

def top_n(num, words):
    '''
    Search for the top "n" frequent words in the documents.
    '''
    results = []

    # We have to change the mapping here because we want to
    # aggregate the overall word frequencies in all the documents
    wrds_frq = words.map(lambda word_path_count: (
        word_path_count[0], word_path_count[1][1]
    ))

    # One more reduction operation to sum up the frequencies.
    wrds_top = wrds_frq.reduceByKey(lambda x, y: x + y)
    for res in wrds_top.takeOrdered(num, key=lambda x: -x[1]):
        results.append((res[0], res[1]))

    print("---------")
    print("TOP TERMS")
    print("---------")
    for res in results:
        print('term: "{0}", count: {1}'.format(res[0], res[1]))

    return map(lambda x: x[0], results)

def search_term(term, words, tmp_dir):
    '''
    Search for a given term in the documents.
    '''
    match = words.filter(lambda word_path_count: word_path_count[0] == term)
    result = match.collect()
    count = 0
    for i in result:
        count += i[1][1]

    print("------")
    print("SEARCH")
    print("------")

    print('term: "{0}", total: {1}'.format(term, count))
    for i in result:
        print(' - path: {0}, hits: {1}'.format(
            i[1][0].replace(tmp_dir, ''), i[1][1]
        ))

def usage():
    '''
    Print the program usage.
    '''
    print(
        "Search for a word or top 'N' terms in a given set of documents.\n\n"
        "USAGE\n\n"
        "   search.py   (N | TERM-1[, TERM-2, [..., TERM-N]]) PATH\n\n"+
        "ARGUMENTS\n\n"
        "   (N | TERM)  If the argument is a number, then search for the\n"
        "               top 'N' terms; otherwise, search for one (or more)\n"
        "               word (or comma-separated list of words); list term\n"
        "               frequency in the documents for both cases.\n\n"
        "   PATH        The path of documents to search from."
    )

def main():
    '''
    Validate command line argumets, build and search the index.
    '''
    if len(sys.argv) < 2:
        usage()
        return

    arg_trm = sys.argv[1]
    doc_dir = sys.argv[2]

    if (arg_trm == "" or doc_dir == ""):
        usage()
        return

    spark_ctx = SparkContext("local", "search-{}".format(arg_trm))

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Add files and build the inverse document index.
        file_rdd = add_files(doc_dir, tmp_dir, spark_ctx)
        wrds_all = tokenize_flip(file_rdd)
        wrds_flt = filter_stop_words(wrds_all)
        wrds_map = init_word_count(wrds_flt)
        wrds_red = reduce_word_count(wrds_map)
        wrds_idx = build_index(wrds_red)

        # Search for top terms or a given set of words,
        # depending on the command line argument.
        if arg_trm.isnumeric():
            terms = top_n(int(arg_trm), wrds_idx)
        else:
            terms = map(lambda x: x.strip(), arg_trm.split(','))

        for term in terms:
            search_term(term.strip(), wrds_idx, tmp_dir)

if __name__ == '__main__':
    main()
