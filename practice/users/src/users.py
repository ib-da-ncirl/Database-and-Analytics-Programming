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
import csv
import datetime as dt
import os.path as osp
import sys
from io import StringIO
import time
import timeit
import xml.etree.ElementTree as eT

import pandas as pd
from bs4 import BeautifulSoup

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

process_limit = sys.maxsize  # set to sys.maxsize to process all records or a smaller value

timeit_count = 0                        # set to desired number of executions, or 0 to disable timing
display_output = (timeit_count == 0)    # display output if not timing execution

csv_source_type = 'sio'        # set to 'file' to store and read csv data from a file, or 'sio' to use a StringIO
csv_file_name = 'users.csv'     # name of csv file for csv_source_type = 'file'

xml_source = 'Users.xml'


def read_attributes_to_array(entry, attributes):
    """
    Read the attributes for an user xml entry
    :param entry: xml entry string
    :param attributes: list of attributes to read
    :return: array of attribute values
    """
    attr_array = []
    for attrib in attributes:
        name = attrib["name"]
        attrib_str = entry.get(name)
        if attrib_str is None:
            if attrib["type"] == dateAttrib:
                attrib_value = 0
            elif attrib["type"] == intAttrib:
                attrib_value = 0
            elif attrib["type"] == floatAttrib:
                attrib_value = 0.0
            elif attrib["type"] == htmlAttrib:
                attrib_value = ""
            else:
                attrib_value = attrib_str
        else:
            attrib_value = attrib_str
        attr_array.append(attrib_value)
    return attr_array


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


def find_highest(title, name, df, column):
    """
    Display the entries with the highest value for the specified column in a DataFrame
    :param title: header to display
    :param name: name of column
    :param df: Pandas DateFrame
    :param column: column to find highest values for
    """
    print_header(title)
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.max.html#pandas.DataFrame.max
    high = df[column].max()
    display(f'highest {name}: {high}')
    highest = df.loc[df[column] == high]
    display(f'highest {name} count: {highest.shape[0]}')
    if not highest.empty:
        display(highest)


def find_lowest(title, name, df, column):
    """
    Display the entries with the lowest value for the specified column in a DataFrame
    :param title: header to display
    :param name: name of column
    :param df: Pandas DateFrame
    :param column: column to find lowest values for
    """
    print_header(title)
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.min.html#pandas.DataFrame.min
    low = df[column].min()
    display(f'lowest {name}: {low}')
    lowest = df.loc[df[column] == low]
    display(f'highest {name} count: {lowest.shape[0]}')
    if not lowest.empty:
        display(lowest)


def get_mean(series):
    """
    Get the mean of a series
    :param series: series to get mean of
    :return: mean value
    """
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.fillna.html#pandas.Series.fillna
    val_series = series.fillna(value=0)
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.mean.html#pandas.Series.mean
    return val_series.mean()


def show_progress(count, bs):
    """
    Show progress
    :param count: count to display
    :param bs: backspace string to overwrite current display
    :return: backspace string to overwrite this display
    """
    # give some in progress feedback
    msg = f'{count}'
    if len(bs) > 0:
        sys.stdout.write(bs)
    bs = '\b' * (len(msg))
    sys.stdout.write(msg)
    sys.stdout.flush()
    time.sleep(0.05)
    return bs


def read_xml(xml_file, attributes):
    """
    Read the attributes for an user xml entry
    :param xml_file: name of xml to read
    :param attributes: list of attributes to read
    :return: array of user dictionaries
    """
    tree = eT.parse(xml_file)
    root = tree.getroot()

    # load into an array of dictionaries ignoring first row as it's not a user
    users_array = []
    bs = ''
    count = 0
    display('user count: ', end='')
    for userXml in root:
        user_arr = read_attributes_to_array(userXml, attributes)
        if int(user_arr[0]) > 0:
            users_array.append(user_arr)
            count = len(users_array)
            if display_output and (count % 100 == 0):
                # give some in progress feedback
                bs = show_progress(count, bs)

            global process_limit
            if count > process_limit:
                break
    show_progress(count, bs)
    display('\n')
    
    return users_array


def export_csv_file(filename, users, header=None):
    """
    Export the user details to a csv file
    :param filename: name of file to export to
    :param users: array of dictionaries with user details
    :param header: Optional header row
    """
    # export to csv file
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if header is not None:
            writer.writerow(header)
        writer.writerows(users)



def main():
    attr_id = {"name": "Id", "type": intAttrib}
    attr_reputation = {"name": "Reputation", "type": intAttrib}
    attr_creation_date = {"name": "CreationDate", "type": dateAttrib}
    attr_display_name = {"name": "DisplayName", "type": strAttrib}
    attr_last_access_date = {"name": "LastAccessDate", "type": dateAttrib}
    attr_website_url = {"name": "WebsiteUrl", "type": strAttrib}
    attr_location = {"name": "Location", "type": strAttrib}
    attr_about_me = {"name": "AboutMe", "type": htmlAttrib}
    attr_views = {"name": "Views", "type": intAttrib}
    attr_up_votes = {"name": "UpVotes", "type": intAttrib}
    attr_down_votes = {"name": "DownVotes", "type": intAttrib}
    attrib_age = {"name": "Age", "type": intAttrib}
    attr_account_id = {"name": "AccountId", "type": intAttrib}
    user_attributes = [
        attr_id, attr_reputation, attr_creation_date, attr_display_name, attr_last_access_date, attr_website_url,
        attr_location, attr_about_me, attr_views, attr_up_votes, attr_down_votes, attrib_age, attr_account_id
    ]
    header = []
    for attrib in user_attributes:
        header.append(attrib["name"])

    global csv_source_type, csv_file_name, xml_source
    if csv_source_type == 'file':
        if not osp.exists(csv_file_name):

            users_array = read_xml(xml_source, user_attributes)

            # export to csv file, so don't have to do conversion again
            export_csv_file(csv_file_name, users_array)

        csv_source = csv_file_name  # set source to file to read
    else:
        # https://docs.python.org/3/library/io.html#io.StringIO
        csv_buffer = StringIO()

        users_array = read_xml(xml_source, user_attributes)

        writer = csv.writer(csv_buffer)
        # writer.writerow(header)
        writer.writerows(users_array)

        csv_buffer.seek(0)

        csv_source = csv_buffer  # set source to StringIO to read

    # load csv data using pandas
    display('load csv')
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    pd_users = pd.read_csv(csv_source, converters={
        attr_creation_date["name"]: pd.to_datetime,
        attr_last_access_date["name"]: pd.to_datetime
    }, names=header)

    if isinstance(csv_source, StringIO):
        csv_source.close()

    # misc
    # display(pd_users)
    # # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.info.html#pandas.DataFrame.info
    # print_header("Info")
    # display(pd_users.info())
    # print_header("Shape")
    # display(pd_users.shape)
    #
    # print_header("Data by column")
    # for head in header:
    #     display(pd_users[head])

    # The oldest user
    print_header("Oldest user")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.min.html#pandas.DataFrame.min
    display(f"date for oldest user: {pd_users[attr_creation_date['name']].min()}")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.idxmin.html#pandas.DataFrame.idxmin
    index = pd_users[attr_creation_date['name']].idxmin()
    display(f'row index of oldest user: {index}')
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.loc.html#pandas.DataFrame.loc
    display(pd_users.loc[index, :])

    # The newest user
    print_header("Newest user")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.max.html#pandas.DataFrame.max
    display(f"date for newest user: {pd_users[attr_creation_date['name']].max()}")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.idxmax.html#pandas.DataFrame.idxmax
    index = pd_users[attr_creation_date['name']].idxmax()
    display(f'row index of newest user: {index}')
    display(pd_users.loc[index, :])

    # Average user age
    print_header("Average user age")
    avg = get_mean(pd_users[attrib_age['name']])
    display(f"average user age: {avg}")

    # User with highest downvote and highest views
    find_highest("User with highest downvote", "downvote", pd_users, attr_down_votes['name'])
    find_highest("User with highest views", "views", pd_users, attr_views['name'])

    # User with highest upvote and lowest views
    find_highest("User with highest upvote", "upvote", pd_users, attr_up_votes['name'])
    find_lowest("User with lowest views", "views", pd_users, attr_views['name'])

    # Users that do not access the website for more than 180 days
    check_time = dt.datetime.now()
    num_days = 180
    date_limit = check_time - dt.timedelta(days=num_days)
    more_than_date_limit = pd_users.loc[pd_users[attr_last_access_date['name']] < date_limit]
    print_header(f"Users that do not access the website for more than {num_days} days: {len(more_than_date_limit)}")
    if not more_than_date_limit.empty:
        display(more_than_date_limit)

    check_time = dt.datetime(2014, 6, 1)
    date_limit = check_time - dt.timedelta(days=num_days)
    more_than_date_limit = pd_users.loc[pd_users[attr_last_access_date['name']] < date_limit]
    print_header(
        f"Users that do not access the website for more than {num_days} days before {check_time} i.e. {date_limit}: {len(more_than_date_limit)}")
    if not more_than_date_limit.empty:
        display(more_than_date_limit)

    # How many people are below 18, from 18-25, 25-35,36-46, above 46
    age_group = pd_users.loc[pd_users[attrib_age['name']] < 18]
    print_header(f"Users that are below 18: {len(age_group)}")
    if not age_group.empty:
        display(age_group)

    age_group = pd_users.loc[(pd_users[attrib_age['name']] >= 18) & (pd_users[attrib_age['name']] <= 25)]
    print_header(f"Users that are from 18-25: {len(age_group)}")
    if not age_group.empty:
        display(age_group)

    age_group = pd_users.loc[(pd_users[attrib_age['name']] >= 25) & (pd_users[attrib_age['name']] <= 35)]
    print_header(f"Users that are from 25-35: {len(age_group)}")
    if not age_group.empty:
        display(age_group)

    age_group = pd_users.loc[(pd_users[attrib_age['name']] >= 36) & (pd_users[attrib_age['name']] <= 46)]
    print_header(f"Users that are from 36-46: {len(age_group)}")
    if not age_group.empty:
        display(age_group)

    age_group = pd_users.loc[pd_users[attrib_age['name']] > 46]
    print_header(f"Users that are above 46: {len(age_group)}")
    if age_group.size > 0:
        display(age_group)

    # Calculate the top 20 frequent locations
    top_count = 20
    locations = pd_users[attr_location['name']]  # series of locations
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.value_counts.html#pandas.Series.value_counts
    locations = locations.value_counts()  # series containing counts of unique locations
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.nlargest.html#pandas.Series.nlargest
    locations = locations.nlargest(n=top_count, keep="first")  # series of largest n elements
    print_header(f"Top {top_count} locations:")
    if not locations.empty:
        display(locations)

    # How many people with the sameWebsiteUrl
    web_url = pd_users[attr_website_url['name']]  # series of website urls
    web_url = web_url.value_counts()  # series containing counts of unique website urls
    print_header(f"Counts of users with same website urls:")
    display(web_url)

    # Users with above the average number of words AboutMe section
    # Users with below the average number of words AboutMe section
    print_header(f"Counts of users with above/below average number of words AboutMe section:")
    about_me = pd_users[attr_about_me['name']]  # series of about me text
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.fillna.html#pandas.Series.fillna
    about_me = about_me.fillna('')          # replace empty with ''

    def strip_html(markup):
        """
        Get the text from a html string
        :param markup: html string
        :return: text
        """
        soup = BeautifulSoup(markup, 'html.parser')
        #display(f'{soup.get_text()}')
        return soup.get_text()

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.map.html#pandas.Series.map
    about_me = about_me.map(strip_html)  # series of just text
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.str.count.html
    about_me_lens = about_me.str.count(r'(\w+)')    # series of word counts

    avg = get_mean(about_me_lens)
    display(f"average AboutMe word count: {avg}")

    def mean_map(x):
        mapped = 'm'
        if x < avg:
            mapped = 'b'
        elif x > avg:
            mapped = 'a'
        return mapped
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.map.html#pandas.Series.map
    about_me_lens = about_me_lens.map(mean_map)  # set value to 'b' if length < mean, 'a' if length > mean or 'm' if == mean
    about_me_counts = about_me_lens.value_counts()  # count unique values
    for x in ['a', 'b', 'm']:
        if x in about_me_counts.index:
            count = about_me_counts[x]
        else:
            count = 0
        if x == 'a':
            display(f"number of users with above average number of words AboutMe section: {count}")
        elif x == 'b':
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

