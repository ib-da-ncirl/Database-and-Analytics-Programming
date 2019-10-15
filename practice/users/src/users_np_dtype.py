# The oldest and newest users
# Average user age
# User with highest downvote and highest views
# User with highest upvote and lowest views
# Users that do not access the website for more than 180 days
# How many people are below 18, from 18-25, 25-35,36-46, above 46
# Calculate the top 20 frequent locations
# How many people with the sameWebsiteUrl
# Users with above the average number of words AboutMe section
# Users with below the average number of words AboutMe section
import sys
import time
import xml.etree.ElementTree as eT
import datetime as dt
import numpy as np
from bs4 import BeautifulSoup
import re
import timeit


# https://docs.python.org/3/library/xml.etree.elementtree.html?highlight=elementtree

# sample entry
# <users>
#     <row Id="-1" Reputation="1" CreationDate="2010-07-19T06:55:26.860" DisplayName="Community"
#         LastAccessDate="2010-07-19T06:55:26.860" WebsiteUrl="http://meta.stackexchange.com/" Location="on the server farm"
#         AboutMe="&lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xA;&#xA;&lt;ul&gt;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackexchange.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xA;&lt;/ul&gt;&#xA;"
#         Views="0" UpVotes="5007" DownVotes="1920" AccountId="-1" />
#     <row Id="2" Reputation="101" CreationDate="2010-07-19T14:01:36.697" DisplayName="Geoff Dalgas"
#         LastAccessDate="2013-11-12T22:07:23.783" WebsiteUrl="http://stackoverflow.com" Location="Corvallis, OR"
#         AboutMe="&lt;p&gt;Developer on the StackOverflow team.  Find me on&lt;/p&gt;&#xA;&#xA;&lt;p&gt;&lt;a href=&quot;http://www.twitter.com/SuperDalgas&quot; rel=&quot;nofollow&quot;&gt;Twitter&lt;/a&gt;&#xA;&lt;br&gt;&lt;br&gt;&#xA;&lt;a href=&quot;http://blog.stackoverflow.com/2009/05/welcome-stack-overflow-valued-associate-00003/&quot;&gt;Stack Overflow Valued Associate #00003&lt;/a&gt;&lt;/p&gt;&#xA;"
#         Views="25" UpVotes="3" DownVotes="0" Age="37" AccountId="2" />
#     ...
# </users>
#

strAttrib = "str"
dateAttrib = "date"
htmlAttrib = "html"
intAttrib = "int"
floatAttrib = "dbl"

process_limit = 100  # set to sys.maxsize to process all records or a smaller value

timeit_count = 10                        # set to desired number of executions, or 0 to disable timing
display_output = (timeit_count == 0)    # display out if not timing execution

string_max = 0


def read_attributes(entry, attributes):
    """
    Read the attributes for an user xml entry
    :param entry: xml entry string
    :param attributes: list of attributes to read
    :return: dictionary of attribute values
    """
    attr_dict = {}
    for attrib in attributes:
        name = attrib["name"]
        attrib_str = entry.get(name)
        if attrib_str is None:
            if attrib["type"] == dateAttrib:
                attr_dict[name] = 0
            elif attrib["type"] == intAttrib:
                attr_dict[name] = 0
            elif attrib["type"] == floatAttrib:
                attr_dict[name] = 0.0
            elif attrib["type"] == htmlAttrib:
                attr_dict[name] = ""
            else:
                attr_dict[name] = attrib_str
        elif attrib["type"] == dateAttrib:
            attr_dict[name] = dt.datetime.fromisoformat(attrib_str)
        elif attrib["type"] == intAttrib:
            attr_dict[name] = int(attrib_str)
        elif attrib["type"] == floatAttrib:
            attr_dict[name] = float(attrib_str)
        elif attrib["type"] == htmlAttrib:
            attr_dict[name] = attrib_str
            alen = len(attrib_str.encode('utf-8'))
            if alen > attrib["size"]:
                raise ValueError(f'size error {alen} > {attrib["size"]} for {name}')
        else:
            attr_dict[name] = attrib_str
            alen = len(attrib_str)
            if alen > attrib["size"]:
                raise ValueError(f'size error {alen} > {attrib["size"]} for {name}')
    return attr_dict


def display(*args, sep=' ', end='\n', file=None):
    """
    Prints the values to a stream, or to sys.stdout by default, if display is enabled
    :param args: what to print
    Optional keyword arguments:
    :param file:  a file-like object (stream); defaults to the current sys.stdout.
    :param sep:   string inserted between values, default a space.
    :param end:   string appended after the last value, default a newline.
    """
    if display_output:
        op = ""
        for arg in args:
            if len(op) > 0:
                op += sep
            op += str(arg)
        print(op, sep=sep, end=end, file=file)


def print_header(title):
    """
    Display a header
    :param title: header title to display
    """
    display(f"\n{title}\n----------------------------------")


def print_array(array, indices):
    """
    Print an array
    :param array: array to print
    :param indices: indices in array to print
    """
    if len(indices) > 0:
        for idx in indices:
            display(array[idx])


def find_highest(title, name, array, column):
    """
    Display the entries with the highest value for the specified column in an array
    :param title: header to display
    :param name: name of column
    :param array: NumPy array
    :param column: column to find highest values for
    """
    print_header(title)
    search_column = array[column]
    high = search_column.max()
    display(f'highest {name}: {high}')
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.where.html#numpy.where
    highest = np.where(search_column == high)
    display(f'highest {name} count: {len(highest[0])}')
    print_array(array, highest[0])


def find_lowest(title, name, array, column):
    """
    Display the entries with the lowest value for the specified column in an array
    :param title: header to display
    :param name: name of column
    :param array: NumPy array
    :param column: column to find lowest values for
    """
    print_header(title)
    search_column = array[column]
    low = search_column.min()
    display(f'lowest {name}: {low}')
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.where.html#numpy.where
    lowest = np.where(search_column == low)
    display(f'lowest {name} count: {len(lowest[0])}')
    print_array(array, lowest[0])


def get_mean(array):
    """
    Get the mean of an array
    :param array: array to get mean of
    :return: mean value
    """
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.nan_to_num.html
    val_series = np.nan_to_num(array, nan=0)
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.mean.html?highlight=mean
    return val_series.mean()


def nlargest_array(array, n):
    """
    Find top most frequent entries in array
    :param array: array to search
    :param n: number of top entries
    :return: tuple of entries and their counts
    """
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.unique.html
    search_counts = np.unique(array, return_counts=True)    # tuple of unique locations and counts
    counts = search_counts[1]
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.argsort.html
    counts_indices = counts.argsort()   # indices of counts sorted in ascending order
    unique_count = len(counts_indices)
    if unique_count < n:
        top_n = search_counts[0][counts_indices[::-1]]  # locations in descending count order
        result = top_n, counts[counts_indices[::-1]]
    else:
        stop = unique_count - n - 1
        top_n = search_counts[0][counts_indices[:stop:-1]]  # locations in descending count order
        result = top_n, counts[counts_indices[:stop:-1]]
    return result


def nlargest(array, n, column):
    """
    Find top most frequent entries in a structured array
    :param array: array to search
    :param n: number of top entries
    :param column: column to search
    :return: tuple of entries and their counts
    """
    return nlargest_array(array[column], n)


def main():
    attrId = {"name": "Id", "type": intAttrib, "size": 0}
    attrReputation = {"name": "Reputation", "type": intAttrib, "size": 0}
    attrCreationDate = {"name": "CreationDate", "type": dateAttrib, "size": 0}
    attrDisplayName = {"name": "DisplayName", "type": strAttrib, "size": 50}
    attrLastAccessDate = {"name": "LastAccessDate", "type": dateAttrib, "size": 0}
    attrWebsiteUrl = {"name": "WebsiteUrl", "type": strAttrib, "size": 256}
    attrLocation = {"name": "Location", "type": strAttrib, "size": 50}
    attrAboutMe = {"name": "AboutMe", "type": htmlAttrib, "size": 4000}
    attrViews = {"name": "Views", "type": intAttrib, "size": 0}
    attrUpVotes = {"name": "UpVotes", "type": intAttrib, "size": 0}
    attrDownVotes = {"name": "DownVotes", "type": intAttrib, "size": 0}
    attribAge = {"name": "Age", "type": intAttrib, "size": 0}
    attrAccountId = {"name": "AccountId", "type": intAttrib, "size": 0}
    userAttributes = [
        attrId, attrReputation, attrCreationDate, attrDisplayName, attrLastAccessDate, attrWebsiteUrl,
        attrLocation, attrAboutMe, attrViews, attrUpVotes, attrDownVotes, attribAge, attrAccountId
    ]

    tree = eT.parse('Users.xml')
    root = tree.getroot()

    # create structured array
    # need to know what sizes to allocate for strings
    # https://docs.scipy.org/doc/numpy/user/basics.rec.html
    type1 = np.dtype([(attrId["name"], np.int64), (attrReputation["name"], np.int64),
                      (attrCreationDate["name"], np.int64), (attrDisplayName["name"], np.str_, attrDisplayName["size"]),
                      (attrLastAccessDate["name"], np.int64), (attrWebsiteUrl["name"], np.str_, attrWebsiteUrl["size"]),
                      (attrLocation["name"], np.str_, attrLocation["size"]),
                      (attrAboutMe["name"], np.str_, attrAboutMe["size"]),
                      (attrViews["name"], np.int64), (attrUpVotes["name"], np.int64),
                      (attrDownVotes["name"], np.int64), (attribAge["name"], np.int64),
                      (attrAccountId["name"], np.int64)
                      ])
    # declare empty array, zero rows but one column
    users = np.empty([0, 1], dtype=type1)

    # load into an array of dictionaries ignoring first row
    bs = ''
    display('user count: ', end='')
    for userXml in root:
        user = read_attributes(userXml, userAttributes)
        if user[attrId["name"]] > 0:
            row = np.array([(user[attrId["name"]],
                             user[attrReputation["name"]],
                             user[attrCreationDate["name"]].timestamp(),
                             user[attrDisplayName["name"]],
                             user[attrLastAccessDate["name"]].timestamp(),
                             user[attrWebsiteUrl["name"]],
                             user[attrLocation["name"]],
                             user[attrAboutMe["name"]],
                             user[attrViews["name"]],
                             user[attrUpVotes["name"]],
                             user[attrDownVotes["name"]],
                             user[attribAge["name"]],
                             user[attrAccountId["name"]]
                             )
                            ], dtype=type1)
            users = np.row_stack((users, row))
            count = users.shape[0]

            if display_output and (count % 100 == 0):
                # give some in progress feedback
                msg = f'{count}'
                if len(bs) > 0:
                    sys.stdout.write(bs)
                bs = '\b' * (len(msg))
                sys.stdout.write(msg)
                sys.stdout.flush()
                time.sleep(0.2)

            global process_limit
            if count > process_limit:
                break
    display('\n')

    # misc
    # display(f'shape {users.shape}')
    # display(users)
    # row = 0
    # display(f'row {row}')
    # display(users[0])
    #
    # name = attrId["name"]
    # display(f'column {name}')
    # display(users[name])

    # The oldest user
    print_header("Oldest user")
    creation = users[attrCreationDate['name']]
    # https://docs.python.org/3.5/library/datetime.html#datetime-objects
    display(f"date for oldest user: {dt.datetime.fromtimestamp(creation.min())}")
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.argmin.html
    index = np.argmin(creation)
    display(f'row index of oldest user: {index}')
    display(users[index])

    # The newest user
    print_header("Newest user")
    display(f"date for newest user: {dt.datetime.fromtimestamp(creation.max())}")
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.argmax.html
    index = np.argmax(creation)
    display(f'row index of newest user: {index}')
    display(users[index])

    # Average user age
    print_header("Average user age")
    avg = get_mean(users[attribAge['name']])
    display(f"average user age: {avg}")

    # User with highest downvote and highest views
    find_highest("User with highest downvote", "downvote", users, attrDownVotes['name'])
    find_highest("User with highest views", "views", users, attrViews['name'])

    # User with highest upvote and lowest views
    find_highest("User with highest upvote", "upvote", users, attrUpVotes['name'])
    find_lowest("User with lowest views", "views", users, attrViews['name'])

    # Users that do not access the website for more than 180 days
    check_time = dt.datetime.now()
    num_days = 180
    date_limit = (check_time - dt.timedelta(days=num_days)).timestamp()
    search_column = users[attrLastAccessDate['name']]
    more_than_date_limit = np.where(search_column < date_limit)
    print_header(f"Users that do not access the website for more than {num_days} days: {len(more_than_date_limit[0])}")
    print_array(users, more_than_date_limit[0])

    check_time = dt.datetime(2014, 6, 1)
    date_limit = (check_time - dt.timedelta(days=num_days)).timestamp()
    search_column = users[attrLastAccessDate['name']]
    more_than_date_limit = np.where(search_column < date_limit)
    print_header(
        f"Users that do not access the website for more than {num_days} days before {check_time} i.e. {date_limit}: {len(more_than_date_limit[0])}")
    print_array(users, more_than_date_limit[0])

    # How many people are below 18, from 18-25, 25-35,36-46, above 46
    search_column = users[attribAge['name']]
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.nonzero.html#numpy.nonzero
    age_group = np.asarray(search_column < 18).nonzero()
    x_indices = age_group[0]
    print_header(f"Users that are below 18: {len(x_indices)}")
    print_array(users, x_indices)

    age_group = np.asarray((search_column >= 18) & (search_column <= 25)).nonzero()
    x_indices = age_group[0]
    print_header(f"Users that are from 18-25: {len(x_indices)}")
    print_array(users, x_indices)

    age_group = np.asarray((search_column >= 25) & (search_column <= 35)).nonzero()
    x_indices = age_group[0]
    print_header(f"Users that are from 25-35: {len(x_indices)}")
    print_array(users, x_indices)

    age_group = np.asarray((search_column >= 36) & (search_column <= 46)).nonzero()
    x_indices = age_group[0]
    print_header(f"Users that are from 36-46: {len(x_indices)}")
    print_array(users, x_indices)

    age_group = np.asarray(search_column > 46).nonzero()
    x_indices = age_group[0]
    print_header(f"Users that are above 46: {len(x_indices)}")
    print_array(users, x_indices)

    # Calculate the top 20 frequent locations
    top_count = 20
    print_header(f"Top {top_count} locations:")
    locations_counts = nlargest(users, top_count, attrLocation['name'])
    for idx in range(len(locations_counts[0])):
        if idx == top_count:
            break
        display(locations_counts[0][idx], locations_counts[1][idx])

    # How many people with the sameWebsiteUrl
    web_url_counts = nlargest(users, sys.maxsize, attrWebsiteUrl['name'])
    print_header(f"Counts of users with same website urls:")
    for idx in range(len(web_url_counts[0])):
        display(web_url_counts[0][idx], web_url_counts[1][idx])

    # Users with above the average number of words AboutMe section
    # Users with below the average number of words AboutMe section
    print_header(f"Counts of users with above/below average number of words AboutMe section:")
    about_me = users[attrAboutMe['name']]  # array of about me text
    about_me = np.where(about_me is np.nan, '', about_me)

    def strip_html(markup):
        """
        Get the text from a html string
        :param markup: html string
        :return: text
        """
        soup = BeautifulSoup(markup, 'html.parser')
        #display(f'{soup.get_text()}')
        return soup.get_text()

    # need to vectorise the function to be able to apply it to the array
    # https://docs.scipy.org/doc/numpy/reference/generated/numpy.vectorize.html#numpy.vectorize
    vfunc = np.vectorize(strip_html)

    about_me = vfunc(about_me)  # array of just text

    # create vectorised version of regex to find all words in a string
    # https://docs.python.org/3/library/re.html
    re_words = re.compile(r'(\w+)')
    vmatch = np.vectorize(lambda x: len(re_words.findall(x)))

    about_me_lens = vmatch(about_me)    # array of word counts

    avg = get_mean(about_me_lens)
    display(f"average AboutMe word count: {avg}")

    # replace < avg values with int32 min value, > avg values with int32 max value and == avg with 0
    about_me_lens = np.where(about_me_lens < avg, np.iinfo(np.int32).min, about_me_lens)
    about_me_lens = np.where(about_me_lens > avg, np.iinfo(np.int32).max, about_me_lens)
    about_me_lens = np.where(about_me_lens == avg, 0, about_me_lens)

    about_me_counts = nlargest_array(about_me_lens, 3)

    idx = 0
    for x in [np.iinfo(np.int32).min, np.iinfo(np.int32).max, 0]:
        if x in about_me_counts[0]:
            count = about_me_counts[1][idx]
        else:
            count = 0
        idx += 1
        if x == np.iinfo(np.int32).max:
            display(f"number of users with above average number of words AboutMe section: {count}")
        elif x == np.iinfo(np.int32).min:
            display(f"number of users with below average number of words AboutMe section: {count}")
        else:
            display(f"number of users with average number of words AboutMe section: {count}")


if __name__ == '__main__':
    if timeit_count == 0:
        main()
    else:
        print(f'\n\nBeginning timeit of {timeit_count} execution(s)')
        timed = timeit.timeit('main()', number=timeit_count)
        print(f'\n\nexecution time: {timed} sec')

