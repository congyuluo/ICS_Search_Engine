import re

from urllib.parse import urlparse

from parse import get_raw_text
from bs4 import BeautifulSoup

# A set of English stopwords as provided on assignment link
stopwords = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'}


def remove_stopwords(text: str) -> str:
    """
    Removes all english stopwords in text
    """
    result = []
    for word in text.split(' '):
        if not word in stopwords:
            result.append(word)
    return " ".join(result)


def tokenizeWithSet(text: str) -> set:
    """
    Runtime complexity: Linear time relative to size of input. Action is performed on each line & each word in line
    I wrote this extra function to tokenize using set for reduced ram usage
    Modified from Assignment 1
    """
    return set([i.lower() for i in re.sub(r'[^a-zA-Z0-9]', ' ', text).split()])


def get_intersection_union(tokenSet1: set, tokenSet2: set) -> int:
    """
    Runtime complexity: Linear time relative to size of input. Performs action of each element in set.
    Returns the intersection and the union of two sets.
    Modified from Assignment 1
    """
    # Determine the sizes of sets
    if len(tokenSet1) < len(tokenSet2):
        smallerSet = tokenSet1
        largerSet = tokenSet2
    else:
        smallerSet = tokenSet2
        largerSet = tokenSet1
    # Memorize initial length of largerSet
    largerSet_original_length = len(largerSet)
    # Add all tokens in smallerSet to largerSet
    for token in smallerSet:
        largerSet.add(token)
    return len(smallerSet) + largerSet_original_length - len(largerSet), len(largerSet)


def token_simularity(tokenSet1: set, tokenSet2: set) -> float:
    """
    Takes in two token sets, compares similarity based on occurrences of words.
    Returns a float indicating similarity between two texts
    """
    intersection, union = get_intersection_union(tokenSet1, tokenSet2)
    return intersection / union


class near_duplicate_db:

    def __init__(self, similarity_cutoff=.9, compare_to_past=50, remove_stopwords=True, ban_limit=3, ban_percentage_limit=.05):
        """
        Class for comparing for near duplicates, compares to past web sites with same netloc
        """
        self.netlocs = {}
        self.similarity_cutoff = similarity_cutoff
        self.compare_to_past = compare_to_past
        self.remove_stopwords=remove_stopwords
        self.ban_limit = ban_limit
        self.ban_percentage_limit = ban_percentage_limit
        self.duplicates = {}
        self.banned_paths = set()
        self.visited_paths = {}

    def add_duplicate(self, url: str):
        """
        Adds a path to duplicates dict
        """
        # Get path of url
        parced = urlparse(url)
        path = "{scheme}://{netloc}/{path}".format(scheme=parced.scheme, netloc=parced.netloc, path=parced.path)
        # Add to duplicates
        if path in self.duplicates.keys():
            self.duplicates[path] += 1
        else:
            self.duplicates[path] = 1

    def check_duplicate(self, url: str, content: bytes) -> (bool, str):
        """
        Checks a new web page against database
        """
        # Get netloc of new web site
        netloc = urlparse(url).netloc
        # Process current content
        current_content = get_raw_text(BeautifulSoup(content, 'html.parser'))
        current_content = ' '.join(current_content)
        # Remove stopwords
        if self.remove_stopwords:
            current_content = remove_stopwords(current_content)
        # Tokenize content
        current_token = tokenizeWithSet(current_content)

        # Detect empty file
        if len(current_token) == 0:
            self.add_duplicate(url)
            return (True, 'Empty token')

        # Check is netloc in database's key
        if netloc in self.netlocs.keys():
            # Limit comparison to last 50
            if len(self.netlocs[netloc]) > self.compare_to_past:
                to_compare = self.netlocs[netloc][-self.compare_to_past:]
            else:
                to_compare = self.netlocs[netloc]

            # Compare token sets
            for url_in_db, token_in_db in to_compare:
                if token_simularity(token_in_db, current_token) > self.similarity_cutoff:
                    # matched
                    self.add_duplicate(url)
                    return True, url_in_db

            # If no match, append to db
            self.netlocs[netloc].append((url, current_token))
            # No match
            return False, None

        # Append current url to db if unseen
        else:
            self.netlocs[netloc] = [(url, current_token)]
            return False, None
