from bs4 import BeautifulSoup
from urllib.parse import urldefrag
from urllib.parse import urlparse
import re

from pipeline import pipeline

# A list of valid paths
def get_links(url: str, content: bytes, debug=False) -> [str]:
    """
    Returns a list of all valid html links
    """
    soup = BeautifulSoup(content, 'html.parser')
    # Referred to Beautifulsoup tutorial in https://pythonprogramminglanguage.com/get-links-from-webpage/
    links = []
    for link in soup.findAll('a'):
        links.append(link.get('href'))

    # Complete imconplete links
    for i in range(len(links)):
        if links[i] and links[i].startswith('//'):
            links[i] = "https:" + links[i]

    # Get source scheme & netloc
    parced_url = urlparse(url)
    head = parced_url.scheme + "://" + parced_url.netloc

    # Complete relative url
    for i in range(len(links)):
        if links[i] and links[i].startswith('/'):
            links[i] = head + links[i]

    if debug:
        filtered_links = [i for i in links if i != None and i[0:4] == 'http']
        print([i for i in links if i not in filtered_links])
        # defragmented result
        defragmented = [urldefrag(i)[0] for i in filtered_links]
        # Remove duplicates & return
        return list(set(defragmented))
    else:
        try:
            # Remove all items that does not begin with 'http'
            filtered_links = [i for i in links if i != None and i[0:4] == 'http']
            # defragmented result
            defragmented = [urldefrag(i)[0] for i in filtered_links]
            # Remove duplicates & return
            return list(set(defragmented))
        except:
            return ['error']


def get_raw_text_old(soup) -> []:
    # Find all text elements in page
    texts = soup.findAll(text=True)
    return texts


def get_raw_text(soup) -> []:
    # Find all text elements in page
    tag = soup.body
    result = []
    if tag == None:
        # print("Using antique method due to method failure")
        texts = soup.findAll(text=True)
        for i in texts:
            result += [x.replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\t', u' ').replace(u'\r', u' ') for x in i.split(" ")]
    else:
        for i in tag.strings:
            result += [x.replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\t', u' ').replace(u'\r', u' ') for x in i.split(" ")]
    return result


def get_headings(soup) -> []:
    # Get all headings by tag
    headings = soup.findAll(["h1", "h2", "h3"])
    headings = [i.text for i in headings]
    result = []
    # Split items
    for i in headings:
        result += [x.replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\t', u' ').replace(u'\r', u' ') for x in
                   i.split(" ")]
    return result


def get_title(soup) -> []:
    if not soup.title:
        return []
    # Get all headings by tag
    title = soup.title.text
    # Split items
    result = [i.replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\t', u' ').replace(u'\r', u' ') for i in title.split(" ")]
    return result


def get_title_text(content: str) -> str:
    """Gets the title of the webpage"""
    soup = BeautifulSoup(content, 'html.parser')
    if soup.title:
        return soup.title.text.replace("\n", " ")
    return None


def get_bold(soup) -> []:
    # Get all headings by tag
    bolds = soup.findAll('b')
    result = []
    # Split items
    for i in bolds:
        try:
            result += [x.replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\t', u' ').replace(u'\r', u' ') for x in
                       i.split(" ")]
        except:
            return []
    return result


def remove_ascii(text: str):
    # Using encoding & decoding technique first
    x = text.encode('ascii', 'ignore').decode()
    # Remove whatever left using my new technique
    return "".join([i for i in x if len(repr(i)) < 4])


def remove_contents(texts: list) -> str:
    texts = [remove_ascii(i.strip()) for i in texts]

    # Remove element without alphaneumeric elements
    regex = re.compile(r'[a-zA-Z0-9]')
    texts = [i for i in texts if regex.search(i)]

    # Remove other undesirable texts
    texts = " ".join([i for i in texts if not (i.startswith('/') or i.startswith('.') or i.startswith('end ')
                                               or i.startswith('wp') or '.com' in i or '.org' in i)])

    texts = texts.replace("[", ' ').replace("]", ' ').replace(":", ' ').replace("'", ' ').replace('"', ' ').replace('`', ' ').replace('|', ' ').replace('_', ' ').replace('~', ' ').replace(',', ' ').replace('-', ' ')
    # Clean up
    regex = re.compile(r'[\\^][a-zA-Z0-9_]*')
    texts = regex.sub(" ", texts)



    # Remove white spaces & left url & markers
    trash = re.compile('http|<|>')
    texts = " ".join([i for i in texts.split(' ') if len(i) > 1 and not trash.search(i)])
    # Return result
    return texts


def get_content_extraction_pipeline() -> pipeline:
    """
    Constructs a pipeline for content extraction
    """
    return pipeline('content_extraction', [get_raw_text, remove_contents], protected=True, error_behavior=['all' for _ in range(2)], error_return_object='')
