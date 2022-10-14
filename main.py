import os
import nltk
import ujson
from nltk.stem.snowball import SnowballStemmer
from pipeline import pipeline
from parse import get_raw_text, remove_contents, get_headings, get_title, get_bold, get_title_text
from index_agent import index_agent
from multiprocessing import Process, Manager
import time
import re
from cython_defs import vector_space_ranking_hybrid
from bs4 import BeautifulSoup
from page_similarity import near_duplicate_db

selected_stopwords = {'an', 'you', 'of', 'it', 'are', 'we', 'he', 'she', 'is', 'the', 'it'}

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

file_location = 'DEV/'
stemmer = SnowballStemmer("english")

weights = {'raw_text_tokens': 1,
          'heading_tokens' : .5,
          'bold_tokens': .5,
          'title_tokens': 2}


def to_lower(sentence: str) -> str:
    """Wrapper function, converts to all lower case"""
    return sentence.lower()


def break_by(tokens: [str]) -> [str]:
    """Break by certain things"""
    result = []
    for token in tokens:
        if re.search(r'/', token):
            result += [i for i in token.split("/") if len(i) > 0]
        elif re.search(r'_', token):
            result += [i for i in token.split("/") if len(i) > 0]
        else:
            result.append(token)

    return result


def remove_punctuations(words: [str]) -> [str]:
    """Wrapper function, remove all punctuations"""
    regex = re.compile(r'[a-zA-Z0-9]')
    texts = [i for i in words if regex.search(i)]
    return texts


def stem_all(tokens: [str]) -> [str]:
    """Wrapper functions, stem all strings in list"""
    return [stemmer.stem(i) for i in tokens]


def get_file(file_location: str) -> str:
    """Gets text information from file at given location"""
    f = open(file_location)
    data = ujson.load(f)
    return data['content']


def remove_junk(tokens: [str]) -> [str]:
    """Remove useless information"""
    tokens = [i for i in tokens if not re.search(r'=', i)]
    return tokens


def make_query_tokenizing_pipeline() -> pipeline:
    """
    Constructs a pipeline for query tokenizing
    """
    return pipeline('tokenize', [remove_contents, to_lower, nltk.word_tokenize, remove_punctuations, break_by, stem_all, remove_junk], protected=False, error_behavior=['all' for _ in range(7)], error_return_object=[])


def make_tokenizing_pipeline() -> pipeline:
    """
    Constructs a pipeline for tokenizing
    """
    return pipeline('tokenize', [remove_contents, to_lower, nltk.word_tokenize, remove_punctuations, break_by, stem_all, remove_junk], protected=False, error_behavior=['all' for _ in range(7)], error_return_object=[])


def add_to_index(reverse_index: dict, serial: int, tokens: [str], multiplier=1):
    """Adds a list of tokens to a partial index"""
    # Iterate over all tokens
    for token in tokens:
        # If token exists
        if token in reverse_index.keys():
            # If serial in token dict
            if serial in reverse_index[token].keys():
                # Add to existing data
                reverse_index[token][serial] += round(1 * multiplier, 3)
            else:
                # Create new data
                reverse_index[token][serial] = round(1 * multiplier, 3)
        else:
            # Create new dict for serial & data
            reverse_index[token] = {serial: round(1 * multiplier, 3)}


def get_all_paths():
    """Serialize all pages, get all pages, urls, and page titles"""
    pages = []
    page_serial, inverse_page_serial, page_titles = dict(), dict(), dict()
    index = 0
    for path in [i for i in os.listdir(file_location) if not i.startswith('.')]:
        site_path = file_location + path + '/'
        if index % 1 == 0:
            print("Current index: " + str(index))
        for page in [i for i in os.listdir(site_path) if not i.startswith('.')]:
            pages.append(site_path + page)
            with open(site_path + page) as f:
                json_content = ujson.load(f)
                url = json_content['url']
                page_title = get_title_text(json_content['content'])
            page_serial[index] = url
            page_titles[index] = page_title
            inverse_page_serial[site_path + page] = index
            index += 1
    return pages, page_serial, inverse_page_serial, page_titles


def get_all_paths_remove_duplicates():
    """Serialize all pages, get all pages, urls, and page titles"""
    nd_db = near_duplicate_db()
    pages = []
    page_serial, inverse_page_serial, page_titles = dict(), dict(), dict()
    index = 0
    for path in [i for i in os.listdir(file_location) if not i.startswith('.')]:
        site_path = file_location + path + '/'
        if index % 1 == 0:
            print("Current index: " + str(index))
        for page in [i for i in os.listdir(site_path) if not i.startswith('.')]:
            with open(site_path + page) as f:
                json_content = ujson.load(f)
                url = json_content['url']
                # Check for near duplicate
                is_duplicate, x = nd_db.check_duplicate(url, json_content['content'])
                if not is_duplicate:
                    page_title = get_title_text(json_content['content'])
                if is_duplicate:
                    print("Duplicate found at: " + url)
            if not is_duplicate:
                pages.append(site_path + page)
                page_serial[index] = url
                page_titles[index] = page_title
                inverse_page_serial[site_path + page] = index
                index += 1
    return pages, page_serial, inverse_page_serial, page_titles


def merge_reverse_index(a: dict, b: dict) -> dict:
    """Merge two partial index, for multiprocessing purposes"""
    # Get all keys in an iterable
    a_keys = set(a.keys())
    # Merge each item of two dicts
    for key in b.keys():
        if key in a_keys:
            a[key].update(b[key])
        else:
            a[key] = b[key]
    return a


def manager_to_dict(x) -> dict:
    """Copies a dictionary"""
    result = dict()
    for key in x.keys():
        result[key] = x[key]
    return result


def multicore_worker(process_index: int, tokenize_ppl: pipeline, paths: [], inverse_page_serial: {}, result_index: {}):
    """A worker process for the multiprocessing indexer"""
    print("Process [{i}] started".format(i=process_index))
    # Create local index
    local_index = dict()
    while len(paths) > 0:
        # Get new path
        try:
            current_path = paths.pop()
        except:
            break

        # Get content of website
        content = get_file(current_path)
        # Make soup
        soup = BeautifulSoup(content, 'html.parser')

        # Collect different types of tokens
        raw_text_tokens = tokenize_ppl.process_item(get_raw_text(soup))
        heading_tokens = tokenize_ppl.process_item(get_headings(soup))
        bold_tokens = tokenize_ppl.process_item(get_bold(soup))
        title_tokens = tokenize_ppl.process_item(get_title(soup))

        # Add tokens to partial index with given weights
        add_to_index(local_index, inverse_page_serial[current_path], raw_text_tokens, multiplier=weights['raw_text_tokens'])
        add_to_index(local_index, inverse_page_serial[current_path], heading_tokens, multiplier=weights['heading_tokens'])
        add_to_index(local_index, inverse_page_serial[current_path], bold_tokens, multiplier=weights['bold_tokens'])
        add_to_index(local_index, inverse_page_serial[current_path], title_tokens, multiplier=weights['title_tokens'])

    print("Process [{i}] moving local variable for return".format(i=process_index))
    for key in local_index.keys():
        result_index[key] = local_index[key]


def multicore_reporting_worker(initial_length: int, paths: [], report_freq=1):
    """Reporting thread for multiprocessing indexer"""
    percentages = set()
    path_size = len(paths)
    while path_size > 1:
        path_size = len(paths)
        completed = initial_length - len(paths)
        percentage = int((completed / initial_length) * 100)
        if (percentage not in percentages) and percentage % report_freq == 0:
            print("{p}% completed [{c}/{t}]".format(p=percentage, c=completed, t=initial_length))
            percentages.add(percentage)


def multicore_indexer(agent: index_agent, all_paths: [], page_serial: dict, inverse_page_serial: dict, process_count=4, batch_size=10000):
    """Multiprocessing indexer"""
    # Create pipeline object
    tokenize_ppl = make_tokenizing_pipeline()
    # Count all paths
    num_paths = len(all_paths)
    print("Total size: {b}".format(b=num_paths))
    # Process paths in batches
    for i in range(0, num_paths, batch_size):
        if i + batch_size > num_paths:
            current_batch = all_paths[i:]
        else:
            current_batch = all_paths[i:i+batch_size]
        print("Current batch size: {b}".format(b=len(current_batch)))
        # Create multiprocessing shared variable manager
        manager = Manager()
        # Create shared variable
        shared_paths = manager.list(current_batch)
        shared_inverse_serial = manager.dict(inverse_page_serial)
        # Create list of local index
        local_indices = [manager.dict() for _ in range(process_count)]
        # Create reporting thread
        jobs = []
        p = Process(target=multicore_reporting_worker, args=(len(shared_paths), shared_paths))
        jobs.append(p)
        p.start()

        # Create processing thread
        for i in range(process_count):
            p = Process(target=multicore_worker,
                        args=(i, tokenize_ppl, shared_paths, shared_inverse_serial, local_indices[i]))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        # Gather all data
        print("Converting local indices")
        local_indices = [manager_to_dict(i) for i in local_indices]
        result = local_indices[0]
        for i, index in enumerate(local_indices[1:]):
            print("Merging local indices: [{i}/{t}]".format(i=i, t=process_count - 1))
            result = merge_reverse_index(result, index)

        # Merge to global index
        agent.merge_to(result)
    # Add page serial to indexing agent
    agent.add_page_serial(page_serial)
    # Process standalone index from full index
    agent.process_standalone_single_index()


def convert_to_html(paths: [str], target_loc= 'web_pages/'):
    """Convert all files to html format"""
    for index, path in enumerate(paths):
        content = get_file(path)
        file = open('{t}/{i}.html'.format(t=target_loc, i=index), 'w')
        file.write(content)
        file.close()


def vector_space_search(ia: index_agent, user_input: str, query_tokenizing_ppl: pipeline) -> [str]:
    """Performs a vector space search with user query"""
    # Split input and tokenize with tokenizing pipeline
    query = query_tokenizing_ppl.process_item(user_input.split(' '))
    # Remove selected stopwords
    stopword_removed = [i for i in query if i not in selected_stopwords]
    # Collect posting
    postings = []
    for i in stopword_removed:
        posting = ia.get_posting(i)
        if posting:
            postings.append(posting)
    # Check if postings are all empty
    is_empty = True
    for i in postings:
        if len(i) > 0:
            is_empty = False
            break
    if is_empty:
        # Try including stopwords
        stopwords_in_query = [i for i in query if i in selected_stopwords]
        for s in stopwords_in_query:
            postings.append(ia.get_posting(s))
        for i in postings:
            if len(i) > 0:
                is_empty = False
                break
        # Return None if still empty
        if is_empty:
            return None
    x = vector_space_ranking_hybrid(postings)

    return x


def init_index_agent():
    """Initialize index agent"""
    # Wipe agent file
    ia = index_agent(init=True)
    # Get all info from files
    all_paths, page_serial, inverse_page_serial, page_titles = get_all_paths()
    a = time.time()
    # Index all websites
    multicore_indexer(ia, all_paths, page_serial, inverse_page_serial, process_count=6, batch_size=10000)
    # Add titles to pages
    ia.add_page_titles(page_titles)
    # Construct tf-idf
    ia.construct_tf_idf()
    # Update cache
    ia.update_cache()
    print("Time used: " + str(time.time() - a))


def terminal_ui():
    """Initialize search engine, enter search UI"""
    # Create Index agent
    ia = index_agent()
    # Create tokenizing pipeline
    query_tokenizing_ppl = make_query_tokenizing_pipeline()
    # Warm up
    vector_space_search(ia, 'hello world', query_tokenizing_ppl)
    # Set page to 0
    current_page = 0
    # Init variable
    result_serials = None
    # Search loop
    while 1:
        user_in = input("Please enter a query (<, > for swapping pages, >quit for quit):")
        if user_in == ">quit":
            break
        # Swap page
        if result_serials and (user_in == "<" or user_in == ">"):
            a = time.time()
            current_page += 1 if user_in == ">" else -1
            # Make sure current Page is in correct boundary
            if current_page < 0:
                current_page = 0
            if current_page >= int(len(result_serials)/10):
                current_page = int(len(result_serials)/10) - 1
            time_taken = time.time() - a
        else:
            a = time.time()
            # Reset Current Page
            current_page = 0
            # Get results
            result_serials = vector_space_search(ia, user_in, query_tokenizing_ppl)
            time_taken = time.time() - a
        # If no result found
        if not result_serials:
            print("___________________________________________________________")
            print(
                "|Time Used: {t}ms | {pc} results|".format(t=1000 * time_taken, pc=0))
            print("No Result Found")
        # If valid result
        else:
            # Print search time
            print("___________________________________________________________")
            print(
                "|Time Used: {t}ms | {pc} results| Page {cp} / {tp}".format(t=1000 * time_taken, pc=len(result_serials),
                                                                            cp=current_page + 1,
                                                                            tp=int(len(result_serials) / 10)))
            # Display page
            if (current_page + 1) * 10 >= len(result_serials):
                display_serials = result_serials[current_page * 10:]
            else:
                display_serials = result_serials[current_page * 10: (current_page + 1) * 10]
            display_urls = ia.get_urls(display_serials)
            display_titles = [ia.get_page_title(i) for i in display_serials]
            for i in range(10):
                try:
                    print("___________________________________________________________")
                    print("No Title Information" if not display_titles[i] else display_titles[i])
                    print("      " + display_urls[i])
                except:
                    break

if __name__ == "__main__":
    terminal_ui()
