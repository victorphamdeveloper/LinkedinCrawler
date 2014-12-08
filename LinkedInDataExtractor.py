import os
import sys
import time
sys.path.insert(0, os.path.abspath(".."))
import Tools
import traceback
import re
import logging
import MySQLdb
import _mysql_exceptions
from collections import OrderedDict
from lxml import etree



import urllib2

##############################################################
# The followings are settings that can be edited:

#Database control-panel
db_address = '127.0.0.1'
db_username = 'admin'
db_password = 'admin'
data_db_name = 'linkedin_hbs1_test'
url_db_name = 'linkedin_test'
db_port = 3306

#How often will you want to 'flush' the list of sql
max_size = 2000

url_output_file = "LinkedInURLFile.txt"

##############################################################
# logger

logger = logging.getLogger('LinkedInDataExtractor')
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


##############################################################
# this part contains constants representing LinkedIn HTML structure and tags

RECOMMENDATION_TEXTS = ('person has recommended', 'people have recommended')

EXP_CLASS_KEYWORDS = ('position', 'experience') # eg: div class="position first experience vevent vcard summary-current"

PRESENT_WORK_KEYWORD = "summary-current" # elements with class containing this keyword denotes current work

EDU_CLASS_KEYWORDS = ('position', 'education') # similar to EXP_CLASS_KEYWORDS

VIEWERS_ALSO_VIEW_PANEL_CLASS = 'leo-module mod-util browsemap'  # HTML class of the right hand side panel showing other profiles

# key = DB column name; value = (HTML class name, flag to enable/disable importing the field into DB)
GENERAL_INFO_COLUMN_MAPPINGS = {"profile_key": ("", 1),
                                "profile_url": ("profile-url", 1),
                                "first_name": ("given-name", 1),
                                "last_name": ("family-name", 1),
                                "curr_job_title": ("headline-title title", 1),
                                "curr_location": ("locality", 1),
                                "curr_industy": ("industry", 1),  # typo in column name
                                "recommendation": ("", 1),
                                "connection": ("overview-connections", 1),
                                "description": (" description summary", 1),
                                "profile_pic": ("profile-picture", 1),  # manual search by id, not class
                                "profile_pic_url": ("", 1)}
WORK_EXP_COLUMN_MAPPINGS = {"profile_key": ("", 1),
                            "work_key": ("", 1),
                            "company_id": ("", 1),
                            "company_name": ("org summary", 1),
                            "company_name_ticker": ("", 1),
                            "company_name_tag": ("company-profile-public", 1),  # href
                            "title": ("title", 1),
                            "title_it_type": ("", 1),
                            "title_mkt_type": ("", 1),
                            "company_org": ("orgstats organization-details", 1), # turning off this means form, industry, size and ticker will not be captured
                            "company_form": ("", 1),
                            "company_industry": ("", 1),
                            "company_size": ("", 1),
                            "start_year": ("dtstart", 1),  # title attribute
                            "start_month": ("dtstart", 1),
                            "end_year": ("dtend", 1),
                            "end_month": ("dtend", 1),
                            "location": ("location", 1),
                            "description": ("description", 1)}
EDUCATION_COLUMN_MAPPINGS = {"profile_key": ("", 1),
                             "edu_key": ("", 1),
                             "school_id": ("summary fn org", 1),
                             "school_id_tag": ("", 1),
                             "degree": ("degree", 1),
                             "degree_type": ("", 1),
                             "major": ("major", 1),
                             "major_type": ("", 1),
                             "start_year": ("dtstart", 1),
                             "end_year": ("dtend", 1),
                             "description": (" desc details-education", 1), # the space before desc is in the actual HTML, not a mistake
                             "activity": ("desc details-education", 1)}


def insert_into_DB(table_name, data_list, flag_dict):
    """
    table_name: DB table name
    data_list: list of dictionaries of data
    flag_dict: the corresponding column_mapping dictionary to check for flags
    Generate SQL statements using column names and values from the dictionaries
    """
    db = MySQLdb.connect(db_address, db_username, db_password, data_db_name, db_port, charset="utf8")
    cursor = db.cursor()
    if data_list:
        column_string = ", ".join(data_list[0].keys())
        value_string = "%s"
        for i in range(len(data_list[0].keys()) - 1):  # make the number of placeholders %s = number of columns
            value_string += ", %s"
        template = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, column_string, value_string)
        # for each dictionary in the list, extract the values only and convert them into a tuple (execute() only accept tuples)
        for row in data_list:
            try:
                # print tuple(flag_dict[key][1] and value or "" for key, value in row.items())
                cursor.execute(template, tuple(flag_dict[key][1] and value or "" for key, value in row.items())) # if flag = 0, import an empty string ""
            except _mysql_exceptions.IntegrityError:
                logger.warning("Profile %s: Error inserting into table %s: Primary key exists" % (row["profile_key"], table_name))
            except:
                logger.exception("Profile %s: Error inserting into table %s: Unexpected error" % (row["profile_key"], table_name))

    db.close()


def process_date(date_string):
    """
    Extract year and month separately
    """
    if len(date_string) > 4:
        m = re.search("(\d{4})\D(\d{2})", date_string) # search for pattern yyyy-mm or yyyy/mm
        year, month = m.groups()
        return year, month
    else:
        return date_string, 0  # if month is not given


def get_local_file_list(folder_path, start_folder = ""):
    """
    Return all files in the given folder and its subfolders
    If start_folder is given, only files in subfolders after start_folder are collected
    Reuse functions from Tools.py
    """
    if start_folder:  # when start_folder is given, set folder_path as the parent of start_folder
        folder_path = os.path.abspath(os.path.join(start_folder, os.pardir))
        if start_folder[-1] not in ("\\", "/"):
            start_folder += "\\"

    if folder_path[-1] not in ("\\", "/"): # check last character of the path
        folder_path += "\\"

    folder_list = Tools.getListofSubFolderFromFolder(folder_path)
    folder_list.append(folder_path)
    folder_list.sort(key=lambda full_path: os.path.basename(os.path.normpath(full_path))[2:])  # sort by folder name, ignoring the worker id part

    file_list = []
    if start_folder:
        start = False
    else:
        start = True

    for folder in folder_list:
        if not start and folder == start_folder:
            start = True
        if start:
            file_list.extend(Tools.getListOfHTMLFromFolder(folder))
    return file_list


def get_profile_key(path):
    """
    If local file: return file name
    If online link: insert into DB to get auto incremented ID
    """

    if "http" in path:  # online profile
        db = MySQLdb.connect(db_address, db_username, db_password, url_db_name, db_port, charset="utf8")
        cursor = db.cursor()
        insert_sql = "INSERT INTO urls (url, status) VALUES (%s, 0)"
        try:
            cursor.execute(insert_sql, path)
        except _mysql_exceptions.IntegrityError:
            pass  # url already in DB, no insertion done

        select_sql = "SELECT id FROM urls WHERE url = %s"
        cursor.execute(select_sql, path)
        (profile_key, ) = cursor.fetchone() # return a tuple, not a single value
        profile_key = int(profile_key)  # need conversion because profile_key is returned as a long integer
    else:  # local file
        # get file name to set profile_key
        file_name = os.path.split(path)[1]
        profile_key = os.path.splitext(file_name)[0]

    return profile_key


def extract_default(ancestor_element, column_mapping_dict):
    """
    Search for nodes (under the ancestor_element only) with HTML class names as specified in the dictionary
    Extract the text of those nodes
    """

    data = OrderedDict() # because the order of entries in this dictionary should be the same as the order of DB columns
    for column, (HTML_class, is_enabled) in column_mapping_dict.items():
        data[column] = ""
        if is_enabled and HTML_class:
            texts = ancestor_element.xpath(".//*[contains(@class, '%s')]/text()" % (HTML_class, ))
            if column in ("description", "connection"): # because data of those columns are in nested tags
                texts.extend(ancestor_element.xpath(".//*[contains(@class, '%s')]//*/text()" % (HTML_class, )))
            data[column] = " ".join(clean_text(text) for text in texts if text is not None)

    return data


########################
# extract general info #
########################
def extract_general_info(tree, profile_key):
    general_info = extract_default(tree, GENERAL_INFO_COLUMN_MAPPINGS)

    general_info["profile_key"] = profile_key

    # extract number of connections
    m = re.search('(\d+)', general_info["connection"])  # get number only, remove text
    if m:
        general_info["connection"] = m.group()

    # extract recommendations
    # TODO: find a better way since this method heavily depends on LinkedIn HTML node structure
    node = tree.xpath(".//*/text()[contains(., '%s') or contains(., '%s')]" % RECOMMENDATION_TEXTS)
    if node and GENERAL_INFO_COLUMN_MAPPINGS["recommendation"][1]:
        text = node[0].getparent().text
        general_info["recommendation"] = re.search('(\d+)', text).group()  # get number only

    # extract profile picture
    pic_node = tree.xpath(".//*[@id='%s']" % (GENERAL_INFO_COLUMN_MAPPINGS["profile_pic"][0],))
    if pic_node:
        general_info["profile_pic"] = 1
        general_info["profile_pic_url"] = pic_node[0].xpath(".//img/@src")[0]  # low-res image, should be replaced by pic_url in extract_url()
    else:
        general_info["profile_pic"] = 0


    # extract profile_url in case the original url is not correct
    # this link only exists if the profile being viewed is not the main profile, eg viewing Singapore profile from in.linkedin.com
    node = tree.xpath(".//link[@rel='canonical']")
    if node:
        general_info["profile_url"] = node[0].get("href")

    return general_info


#########################
# extract work experience
#########################
def extract_work_exp(tree, profile_key):
    temp_exp_list = []

    work_exp_elements = tree.xpath(".//*[contains(@class, '%s') and contains(@class, '%s')]" % EXP_CLASS_KEYWORDS)
    count = 1
    for work_exp_element in work_exp_elements:
        work_exp = extract_default(work_exp_element, WORK_EXP_COLUMN_MAPPINGS)

        work_exp["profile_key"] = profile_key
        work_exp["work_key"] = count

        #Special cases when data needs additional processing

        #company id in href attribute
        company_href = work_exp_element.xpath(".//*[@class='%s']" % WORK_EXP_COLUMN_MAPPINGS["company_name_tag"][0])
        work_exp["company_name_tag"] = company_href and company_href[0].get("href").replace("?trk=ppro_cprof", "") or ""

        # need processing because of multiple format: year only or month-year
        start_date = work_exp_element.xpath(".//*[@class='%s']/@title" % WORK_EXP_COLUMN_MAPPINGS["start_year"][0])
        if start_date:
            work_exp["start_year"], work_exp["start_month"] = process_date(start_date[0])

        is_present = PRESENT_WORK_KEYWORD in work_exp_element.get("class") # better than searching for dtstamp because sometimes linkedin only shows "Currently holds this position"
        if is_present:
            work_exp["end_year"] = work_exp["end_month"] = -99
        else:
            end_date = work_exp_element.xpath(".//*[@class='%s']/@title" % WORK_EXP_COLUMN_MAPPINGS["end_year"][0])
            if end_date:
                work_exp["end_year"], work_exp["end_month"] = process_date(end_date[0])

        # extract company form, size, industry and ticker from one HTML field
        if work_exp["company_org"]:
            company_info = work_exp["company_org"].split(";")
            if len(company_info) > 2:
                work_exp["company_form"], work_exp["company_size"] = [data.strip() for data in company_info[0:2]]
                work_exp["company_industry"] = company_info[-1].strip()
            if len(company_info) == 4:
                work_exp["company_name_ticker"] = company_info[2]

        count += 1
        temp_exp_list.append(work_exp)

    return temp_exp_list


###################
# extract education
###################
def extract_education(tree, profile_key):
    temp_edu_list = []

    education_elements = tree.xpath(".//*[contains(@class, '%s') and contains(@class, '%s')]" % EDU_CLASS_KEYWORDS)
    count = 1
    for education_element in education_elements:
        education = extract_default(education_element, EDUCATION_COLUMN_MAPPINGS)

        # additional processing
        education["profile_key"] = profile_key
        education["edu_key"] = count

        # extract activities
        if EDUCATION_COLUMN_MAPPINGS["activity"]:
            texts = education_element.xpath(".//*[@name='activities']/text()")
            # texts.extend(education_element.xpath(".//*[@name='activities']//*/text()"))
            education["activity"] = " ".join(clean_text(text) for text in texts if text is not None)

        # extract year from @title attribute
        start_year = education_element.xpath(".//*[@class='%s']/@title" % EDUCATION_COLUMN_MAPPINGS["start_year"][0])
        if start_year:
            education["start_year"], temp = process_date(start_year[0])
        end_year = education_element.xpath(".//*[@class='%s']/@title" % EDUCATION_COLUMN_MAPPINGS["end_year"][0])
        if end_year:
            education["end_year"], temp = process_date(end_year[0])

        count += 1
        temp_edu_list.append(education)

    return temp_edu_list

def clean_text(text):
    """
    Simple method to replace multiple whitespace with only one space and encode with utf8
    """
    return " ".join(text.encode("utf-8").split())


def extract_url(tree_element, url_dict):
    """
    Find all profile url and write into the dictionary with key = url, value = 1
    """
    browsemap_element = tree_element.find(".//div[@class='%s']" % VIEWERS_ALSO_VIEW_PANEL_CLASS)
    pattern = 'shrink_\d+_\d+/'  #eg: shrink_40_40/

    if browsemap_element is not None:
        url_elements = browsemap_element.findall(".//a")
        for e in url_elements:
            url = e.get("href").replace("?trk=pub-pbmap", "")
            img_element = e.find(".//img")
            if img_element is not None:  # each profile has 2 a href elements, but only one of them is a img
                pic_url = img_element.get("data-li-src")
                if pic_url is None:  # only store real profile picture
                    url_dict[url] = ""
                else:
                    pic_url = re.sub(pattern, "", pic_url)
                    url_dict[url] = pic_url

    return url_dict


def print_url(url_dict):
    """
    Print all URLs on the right hand side into a text file
    """
    with open(url_output_file, 'w') as f:
        for url, pic_url in url_dict.items():
            url.encode('utf-8')
            pic_url.encode('utf-8')
            f.write("%s, %s\n" % (url, pic_url))


def usage():
    usage = """
    Usage:
    -h --help                       Display this manual
    -f --folder [folder path]       Extract data from HTML files in the given folder and sub-folders
    -sf --startfolder [folder path] Extract data from HTML files in folders starting from the given folder
    -l --link [web link]            Extract data from an online profile
    """
    print usage


def main(profile_list):
    general_info_list = []
    work_exp_list = []
    education_list = []
    url_dict = {}
    pic_url_dict = {}
    start_time = time.time()
    total_time = 0

    for input_file in profile_list:
        try:
            #### Parse file ####
            parser = etree.HTMLParser()
            try:
                tree = etree.parse(input_file, parser)
            except etree.XMLSyntaxError:
                # This exception is only raised when the file is heavily damaged
                # For small errors such as missing closing tag, only that node is ignored
                logger.warning("Profile %s: Error reading file: HTML syntax error" % (input_file, ))
                continue
            except IOError:
                logger.exception("Profile %s: Error reading file: Cannot open file/URL" % input_file)
                continue
            except:
                logger.exception("Profile %s: Error reading file: Unexpected error" % input_file)
                continue

            profile_key = get_profile_key(input_file)

            #### Extract data and insert into DB ####
            temp_general_info = extract_general_info(tree, profile_key)
            temp_work_exp_list = extract_work_exp(tree, profile_key)
            temp_education_list = extract_education(tree, profile_key)

            # only adding to the global list when no exception occurred
            general_info_list.append(temp_general_info)
            work_exp_list.extend(temp_work_exp_list)
            education_list.extend(temp_education_list)

            # import when we have enough profiles
            if len(general_info_list) >= max_size:
                insert_into_DB("general_info", general_info_list, GENERAL_INFO_COLUMN_MAPPINGS)
                insert_into_DB("education", education_list, EDUCATION_COLUMN_MAPPINGS)
                insert_into_DB("work_exp", work_exp_list, WORK_EXP_COLUMN_MAPPINGS)
                elapsed_time = time.time() - start_time
                total_time += elapsed_time
                start_time = time.time()
                print "Inserted: %d profiles, %d education rows, %d work rows in %f seconds" % (len(general_info_list), len(education_list), len(work_exp_list), elapsed_time)
                # clear the lists after inserting into DB
                general_info_list = []
                education_list = []
                work_exp_list = []

            #### Extract URLs ####
            url_dict = extract_url(tree, url_dict)
            # pic_url_dict = extract_pic_url(tree, profile_key, pic_url_dict)

        except:
            logger.exception("Profile %s: Unexpected error:" % (input_file, ))

    ##### All files processed
    ### Insert the last batch of data
    insert_into_DB("general_info", general_info_list, GENERAL_INFO_COLUMN_MAPPINGS)
    insert_into_DB("education", education_list, EDUCATION_COLUMN_MAPPINGS)
    insert_into_DB("work_exp", work_exp_list, WORK_EXP_COLUMN_MAPPINGS)

    ### Print the URL file
    print_url(url_dict)

    elapsed_time = time.time() - start_time
    total_time += elapsed_time
    logger.info("Inserted: %d profiles, %d education rows, %d work rows in %d seconds" % (len(general_info_list), len(education_list), len(work_exp_list), elapsed_time))
    logger.info("Number of unique URLs: %d" % (len(url_dict), ))
    logger.info("Total: %f seconds" % total_time)

    return general_info_list, work_exp_list, education_list


########################
#  Main
########################
if __name__ == '__main__':
    try:
        option = sys.argv[1]
    except:
        option = "-h"
    if option in ("-h", "--help"):
        usage()
        sys.exit()
    elif option in ("-f", "--folder"):
        profile_list = get_local_file_list(sys.argv[2])
    elif option in ("-sf", "--startfolder"):
        profile_list = get_local_file_list("", sys.argv[2])
    elif option in ("-l", "--link"):
        profile_list = [sys.argv[2]]
    elif option == "-d":  # debug, run from IDE
        test_xml = 'E:\\LTVPCrawlerV2\\WorkStation_LinkedIn_6_1\\1_1387425098\\301748140.html'
        # test_online_xml = "http://sg.linkedin.com/pub/khanh-vu-tran-doan"
        test_online_xml = "http://sg.linkedin.com/pub/khanh-vu-tran-doan/31/602/8b?trk=pub-pbmap"
        # test_online_xml = "http://jp.linkedin.com/pub/%E3%81%82%E3%82%86%E3%81%BF-%E8%B0%B7/44/639/148?trk=pub-pbmap"
        profile_list = [test_xml]
    else:
        print "Invalid argument"
        usage()
        sys.exit(2)

    main(profile_list)


