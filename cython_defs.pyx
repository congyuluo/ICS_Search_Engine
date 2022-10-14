# Cython library optimized from python code for speed improvement. Approved by professor.
import math
from pipeline import pipeline
from index_agent import index_agent

selected_stopwords = {'an', 'you', 'of', 'it', 'are', 'we', 'he', 'she', 'is', 'the', 'it'}

# Referenced & modified from technique from https://stackoverflow.com/questions/18424228/cosine-similarity-between-2-number-lists
cdef float cosine_similarity_2(list v1, list v2):
    """Compute cosine similarity of v1 to v2"""
    cdef float sumxx, sumxy, sumyy, x, y
    sumxx, sumxy, sumyy = 0, 0, 0
    cdef int v1_len, i
    v1_len = len(v1)
    for i in range(len(v1)):
        x = v1[i]; y = v2[i]
        sumxx += x*x
        sumyy += y*y
        sumxy += x*y
    return sumxy/math.sqrt(sumxx*sumyy)

def length_normalize(list a) -> list:
    """Length normalization to a list"""
    cdef float temp, i
    temp = math.sqrt(sum([i**2 for i in a]))
    return [i / temp for i in a]

def log_frequency(list a) -> list:
    """Computes log frequency of every element"""
    cdef float i
    return [1 + math.log(abs(i), 10) for i in a]

def get_term_idf(list posting):
    """Gets the idf of a single term"""
    return math.log(55393.0 / len(posting), 10)

def get_union(list posting_1, list posting_2, int posting_1_len, int posting_2_len) -> []:
    """Gets the union of two lists"""
    cdef int index_1, index_2, item_1, item_2
    index_1, index_2 = 0, 0

    result = []
    # Iterate over both posting lists
    while index_1 < posting_1_len and index_2 < posting_2_len:
        # Get current item
        item_1 = posting_1[index_1]
        item_2 = posting_2[index_2][0]
        # Compare items
        if item_1 == item_2:
            # Append to result list
            result.append(item_1)
            # Increment index
            index_1 += 1
            index_2 += 1
        elif item_1 < item_2:
            # Increment index
            index_1 += 1
        else:
            # Increment index
            index_2 += 1
    return result

def get_score_sum(list union_list, list postings, int posting_1_len, int posting_2_len) -> []:
    """Gets the sum of scores of the union list & postings list"""
    cdef int index_1, index_2, item_1, item_2
    index_1, index_2 = 0, 0

    result = []
    # Iterate over both posting lists
    while index_1 < posting_1_len and index_2 < posting_2_len:
        # Get current item
        item_1 = union_list[index_1]
        item_2 = postings[index_2][0]
        # Compare items
        if item_1 == item_2:
            # Append to result list
            result.append(postings[index_2][1])
            # Increment index
            index_1 += 1
            index_2 += 1
        elif item_1 < item_2:
            # Increment index
            index_1 += 1
        else:
            # Increment index
            index_2 += 1
    return result

def get_ranked_doc_ids(list postings) -> [int]:
    """Takes in a list of postings and returns with a list of page serials"""
    # Sort postings by ascending order
    postings.sort(key=len)
    # Initialize union list as contents in postings[0]
    union = [i[0] for i in postings[0]]
    # Iteratively get union of postings
    for i in postings[1:]:
        union = get_union(union, i, len(union), len(i))

    cdef int index, temp

    scores = []
    union_size = len(union)
    # Get the sum of scores for each document
    for i in postings:
        scores.append(get_score_sum(union, i, union_size, len(i)))
    index = 0
    result = {}

    # Calculated score sum from processed list
    while index < union_size:
        temp = 0
        for i in scores:
            temp += i[index]
        result[union[index]] = temp
        index += 1
    # Sort by score and return ranked page serials
    return [i[0] for i in sorted(result.items(), key=lambda kv: kv[1], reverse=True)]


def vector_space_ranking_2(list postings) -> [int]:
    """Calculates ranking based on cosine similarity of each document in union of documents"""
    # Sort postings by ascending order & get union of all postings
    postings.sort(key=len)
    union = [i[0] for i in postings[0]]
    for i in postings[1:]:
        union = get_union(union, i, len(union), len(i))

    postings_in_union = []
    cdef int union_size, I
    union_size = len(union)
    # Get tf-idf of all elements
    for posting in postings:
        postings_in_union.append(get_score_sum(union, posting, union_size, len(posting)))

    # Prepare query matrix
    query_matrix = length_normalize([1 + get_term_idf(posting) for posting in postings])

    # Invert the list
    document_similarity = {}
    I = 0

    while I < union_size:
        # Prepare each matrix and calculate distance to query matrix
        document_similarity[union[I]] = cosine_similarity_2(
            length_normalize([x[I] for x in postings_in_union]), query_matrix)
        I += 1

    # Return serial of pages ranked in decreasing order of cosine_similarity
    return [i[0] for i in sorted(document_similarity.items(), key=lambda kv: kv[1], reverse=True)]


def vector_space_ranking_hybrid(list postings) -> [int]:
    """Calculates ranking based on cosine similarity of each document in union of documents"""
    # Sort postings by ascending order & get union of all postings
    postings.sort(key=len)
    union = [i[0] for i in postings[0]]
    for i in postings[1:]:
        union = get_union(union, i, len(union), len(i))

    postings_in_union = []
    cdef int union_size, I
    union_size = len(union)
    # Get tf-idf of all elements
    for posting in postings:
        postings_in_union.append(get_score_sum(union, posting, union_size, len(posting)))

    # Prepare query matrix
    query_matrix = length_normalize([1 + get_term_idf(posting) for posting in postings])

    # Invert the list
    document_similarity = {}
    I = 0

    # while I < union_size:
    #     # Prepare each matrix and calculate distance to query matrix
    #     document_similarity[union[I]] = cosine_similarity_2(
    #         length_normalize([x[I] for x in postings_in_union]), query_matrix)
    #     I += 1

    while I < union_size:
        temp = [x[I] for x in postings_in_union]
        document_similarity[union[I]] = sum(temp) + 2.5 * cosine_similarity_2(
            length_normalize([x[I] for x in postings_in_union]), query_matrix)
        I += 1

    # Return serial of pages ranked in decreasing order of cosine_similarity
    return [i[0] for i in sorted(document_similarity.items(), key=lambda kv: kv[1], reverse=True)]
