import os
import ujson
import math

# Parameters for index agent file locations
index_agent_file_loc = 'index_agent.json'
index_loc = 'reverse_index.txt'
standalone_index_loc = 'reverse_standalone_index.txt'
temp_index_loc = 'temp_index.txt'
# For faster processing speed
total_doc_count = 55393


def merge_reverse_index(a: dict, b: dict) -> dict:
    """Merges two dictionaries"""
    a.update(b)
    return a


class index_agent:

    def __init__(self, init=False):
        """An manager object for file system & index"""
        # Check if database directory exsists
        if not (os.path.isfile(index_agent_file_loc) and os.path.isfile(index_loc)):
            # If not, construct database
            init = True

        if init:
            print("Warning, initializing index!")
            # Remove database if exists
            if os.path.isfile(index_agent_file_loc):
                os.remove(index_agent_file_loc)
            if os.path.isfile(index_loc):
                os.remove(index_loc)
            # Store empty dict to json
            store_dict = {'single_index_table': {}, 'standalone_single_table_index': {}, 'cache': {}, 'standalone_cache': {}, 'page_serial': {}, 'page_titles': {}}
            with open(index_agent_file_loc, 'w') as file:
                ujson.dump(store_dict, file)
            f = open(index_loc, "w+")
            f.close()
        else:
            print("Using existing index at {l1} and configuration at {l2}".format(l1=index_loc, l2=index_agent_file_loc))

        # Load data from json file
        f = open(index_agent_file_loc)
        store_dict = ujson.load(f)
        f.close()
        # This is the main index of the search engine
        self.reverse_index = open(index_loc, 'r')
        # This is a simplified index containing only page serial
        self.standalone_index = open(standalone_index_loc, 'r')
        # This is a reference table from page serial to posting location in line
        self.single_table_index = store_dict['single_index_table']
        # This is a reference table from page serial to posting location in line
        self.standalone_single_table_index = store_dict['standalone_single_table_index']
        # This is a cache containing 100 postings, approved by professor
        self.cache = store_dict['cache']
        # This is a cache of standalone index, containing same 100 postings as main cache
        self.standalone_cache = store_dict['standalone_cache']
        # This is a reference table from serial to url
        self.page_serial = {int(key): store_dict['page_serial'][key] for key in store_dict['page_serial'].keys()}
        # This is a reference table from serial to page titles
        self.page_titles = {int(key): store_dict['page_titles'][key] for key in store_dict['page_titles'].keys()}

    def get_standalone_posting(self, index: str) -> []:
        """Gets simplified postings for given page serial"""
        # Try cache first
        if index in self.standalone_cache:
            return self.standalone_cache[index]
        # Load data from file
        if index in self.standalone_single_table_index:
            # Seek to line
            self.standalone_index.seek(self.standalone_single_table_index[index])
            # Decode string into 2d list from read information
            return ujson.loads(self.standalone_index.readline())
        return None

    def get_posting(self, index: str) -> []:
        """Gets full posting for given page serial"""
        # Try cache first
        if index in self.cache:
            return self.cache[index]
        # Load data from file
        if index in self.single_table_index:
            # Seek to line
            self.reverse_index.seek(self.single_table_index[index])
            # Decode string into 2d list from read information
            return ujson.loads(self.reverse_index.readline())
        return None

    def get_page_title(self, serial: int) -> str:
        """Gets page title for given page serial"""
        if serial in self.page_titles:
            return self.page_titles[serial]
        return None

    def merge_to(self, new_dict: dict):
        """Merge a partial index into main index file"""
        # Init variables
        global_index, global_len = 0, len(self.single_table_index)
        new_index, new_len = 0, len(new_dict)
        # Get iter objects for existing reference tables from serial to location
        iter_global_dict, iter_new_dict = iter(sorted(self.single_table_index.keys())), iter(sorted(new_dict.keys()))
        if len(self.single_table_index) == 0:
            global_key = None
        else:
            global_key = next(iter_global_dict)
        new_key = next(iter_new_dict)
        # Create temp file for storing the new merged index
        merged = open(temp_index_loc, "w+")
        # Create new reference table from serial to location
        new_single_table_index = dict()
        # Index for keeping write locations
        merged_write_index = 0
        # Iterate through both index
        while global_index < global_len or new_index < new_len:
            # Decide to merge or to load one of the postings
            # Global index done
            if global_index >= global_len:
                operation = 'dump_new'
            # New index done
            elif new_index >= new_len:
                operation = 'dump_global'
            # Compare keys
            else:
                if not global_key:
                    operation = 'dump_new'
                else:
                    if global_key < new_key:
                        operation = 'dump_global'
                    elif global_key > new_key:
                        operation = 'dump_new'
                    else:
                        operation = 'merge'

            # Dump global postings
            if operation == 'dump_global':
                # Store index location
                new_single_table_index[global_key] = merged_write_index
                # Seek posting from global index file
                self.reverse_index.seek(self.single_table_index[global_key])
                # Load data
                item = self.reverse_index.readline().replace('\n', '')
                # Increment global index
                global_index += 1
                if global_index < global_len:
                    global_key = next(iter_global_dict)
            # Dump new postings
            elif operation == 'dump_new':
                # Store index location
                new_single_table_index[new_key] = merged_write_index
                # Get posting
                item = dict(sorted(new_dict[new_key].items()))
                # Increment new index
                new_index += 1
                if new_index < new_len:
                    new_key = next(iter_new_dict)
            # Merge both postings
            else:
                # Store index location
                new_single_table_index[global_key] = merged_write_index
                # Get both postings
                self.reverse_index.seek(self.single_table_index[global_key])
                item_1 = ujson.loads(self.reverse_index.readline())
                item_1 = {pair[0]: pair[1] for pair in item_1}
                item_2 = new_dict[new_key]
                # Merge both postings and sort
                item = dict(sorted(merge_reverse_index(item_1, item_2).items()))
                # Increment both index
                global_index += 1
                new_index += 1
                if global_index < global_len:
                    global_key = next(iter_global_dict)
                if new_index < new_len:
                    new_key = next(iter_new_dict)

            # Process data into json string
            if operation != 'dump_global':
                item = ujson.dumps([i for i in item.items()])
            # Write information
            merged.write(item + "\n")
            merged_write_index += len(item + '\n')

        # Close new file
        merged.close()
        # Update single table index
        self.single_table_index = new_single_table_index
        # Store updates to json
        self.update_json_config()
        # Delete old reverse index & move new one
        self.reverse_index.close()
        os.remove(index_loc)
        os.rename(temp_index_loc, index_loc)
        # Open new reverse index
        self.reverse_index = open(index_loc, 'r')

    def process_standalone_single_index(self):
        """Process standalone index from full index"""
        # Create standalone table index
        standalone_single_table_index = dict()
        # Counter variable
        index = 0
        # Create file
        if os.path.isfile(standalone_index_loc):
            os.remove(standalone_index_loc)
        f = open(standalone_index_loc, 'w')
        # Iterate over data
        for key in self.single_table_index:
            standalone_single_table_index[key] = index
            data = self.get_posting(key)
            data = [i[0] for i in data]
            data = ujson.dumps(data) + '\n'
            f.write(data)
            index += len(data)
        self.standalone_single_table_index = standalone_single_table_index
        self.update_json_config()

    def add_page_serial(self, page_serial_in: dict):
        """Add page serial lookup table"""
        self.page_serial = {int(key): page_serial_in[key] for key in page_serial_in.keys()}
        self.update_json_config()

    def add_page_titles(self, page_titles_in: dict):
        """Add page title lookup table"""
        self.page_titles = page_titles_in
        self.update_json_config()

    def update_json_config(self):
        """Stores all index agent files to json"""
        store_dict = {'single_index_table': self.single_table_index, 'standalone_single_table_index': self.standalone_single_table_index, 'cache': self.cache, 'standalone_cache': self.standalone_cache, 'page_serial': self.page_serial, 'page_titles': self.page_titles}
        with open(index_agent_file_loc, 'w') as file:
            ujson.dump(store_dict, file)

    def construct_tf_idf(self):
        """Process tf-idf score for the main index"""
        # Create temp file
        converted = open(temp_index_loc, "w+")
        new_single_table_index = dict()
        current_write_index = 0
        # Iterate over all keywords in index
        for key in self.single_table_index.keys():
            # Get posting information
            posting = self.get_posting(key)
            # Calculate idf score
            idf = math.log(55393 / len(posting), 10)
            # Multiply posting by idf score
            for i in posting:
                # Add penalty for no title
                if self.get_page_title(i[0]) == None:
                    i[1] = round((1 + math.log(i[1], 10)) * idf, 4) * .5
                else:
                    i[1] = round((1 + math.log(i[1], 10)) * idf, 4)
            # Append information to new table index
            new_single_table_index[key] = current_write_index
            # Convert postings to string for storage
            item = ujson.dumps(posting)
            # Write
            converted.write(item + "\n")
            # Increment current write index
            current_write_index += len(item + '\n')
        # Close new file
        converted.close()
        # Update single table index
        self.single_table_index = new_single_table_index
        # Store updates to json
        self.update_json_config()
        # Delete old reverse index & move new one
        self.reverse_index.close()
        os.remove(index_loc)
        os.rename(temp_index_loc, index_loc)
        # Open new reverse index
        self.reverse_index = open(index_loc, 'r')

    def update_cache(self, n_largest=100):
        """Update cache from main index"""
        # Declare list for sizes of all postings
        sizes = {}
        # Get length of each posting
        for key in self.single_table_index.keys():
            sizes[key] = len(self.get_posting(key))
        # Get n largest postings
        to_cache = [i[0] for i in sorted(sizes.items(), key=lambda kv: kv[1], reverse=True)[:n_largest]]
        # Init cache
        self.cache = {}
        self.standalone_cache = {}
        # Load to cache
        for i in to_cache:
            self.cache[i] = self.get_posting(i)
            self.standalone_cache[i] = [x[0] for x in self.cache[i]]
        # Update config file
        self.update_json_config()

    def get_urls(self, serials: [int]) -> [str]:
        """Get urls of pages"""
        return [self.page_serial[i] for i in serials]

