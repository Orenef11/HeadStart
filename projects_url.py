# coding=utf-8
"""
    Retrieving information for each project.
    For each project, all meta data is removed.
"""
from requests import get
from lxml import etree
from io import StringIO
from urllib.request import urlparse
from headstart_project import HeadStartProject
from pandas import DataFrame, concat
from os import path, makedirs
from time import sleep, clock
from pickle import dump, load
from copy import copy
from datetime import datetime
from random import uniform
import logger
import logging
from traceback import print_exc
URL = 'https://www.headstart.co.il/projects.aspx'
ENDING_UP_PROJECTS_URL = \
    'https://www.headstart.co.il/cat-93-%D7%92%D7%99%D7%99%D7%A1%D7%95_%D7%91%D7%94%D7%A6%D7%9C%D7%97%D7%94.aspx'
LIVE_PROJECT_PICKLE_PATH = 'lives_projects_data.pickle'
FINISHED_PROJECT_PICKLE_PATH = 'older_projects_data.pickle'
ERROR_LOG_PATH = 'error_log.log'
PROGRAM_LOG_PATH = 'program_log.log'
# While there is an error and the software stops, and more than one day has passed, the projects we have not seen will
#  be marked as projects for which we have not been able to find information from the beginning.
BREAK_TIME = 1
REWARD_NUMS = 30

LIVE_PROJECTS_FLAG = True
DEBUG = False
SLEEPING_FLAG = True


class ProjectsURL(object):
    """
        Extracting all projects by category and sub-category.
        For each sub-category, find the page where the projects are relevant to him.
    """

    def __init__(self, projects_url, images_and_videos_folder_path, csv_headers_list, csv_pickle_path, reward_size,
                 is_break_between_runs_flag=False, size_projects_in_line=4, num_categories_in_main_page=3,
                 live_projects_flag=True):
        url_parser = urlparse(projects_url)
        self.__absolute_url = url_parser.scheme + '://' + url_parser.netloc
        self.__sub_categories_dict = {}
        self.__root = etree.parse(StringIO(get(projects_url).text), etree.HTMLParser()).getroot()
        self.__get_projects_page = 'getajaxproj.aspx?id={}&page={}'
        self.__images_and_videos_folder_path = images_and_videos_folder_path
        self.__csv_headers_list = csv_headers_list
        with open(csv_pickle_path, 'rb') as fp:
            self.__project_data_frame = DataFrame(load(fp))
        self.__project_names_list = list(self.__project_data_frame['Project-name'])
        self.__csv_pickle_path = csv_pickle_path
        self.__reward_size = reward_size
        self.__projects_in_main_page_indexes_dict = {'0': 'Team-selection', '1': 'About_to_end', '2': 'Popular',
                                                     '3': 'New'}
        self.__projects_in_main_page_indexes_reverse_dict = dict([(value, key) for key, value in
                                                                  self.__projects_in_main_page_indexes_dict.items()])
        self.__projects_in_main_page_dict = {'Team-selection': [], 'About_to_end': [], 'Popular': [], 'New': []}
        self.__projects_in_main_pages(size_projects_in_line, num_categories_in_main_page)
        self.__is_break_between_runs = is_break_between_runs_flag
        self.__is_live_projects_flag = live_projects_flag
        self.__projects_not_saved_list = []
        if live_projects_flag:
            self.__find_all_categories()

    def __find_all_categories(self):
        for element in self.__root.findall(".//ul[@class='sidenav']/li"):
            category_name = element.find('.//a').text
            if category_name is not None:
                category_name = category_name.strip()
            else:
                continue
            self.__sub_categories_dict[category_name] = {}
            empty_category_flag = True
            for sub_element in element.findall('.//ul/li/a'):
                empty_category_flag = False
                sub_category = sub_element.text
                if sub_category is not None:
                    sub_category = sub_category.strip()
                else:
                    continue
                self.__sub_categories_dict[category_name][sub_category] = sub_element.attrib['href']

            if empty_category_flag is True:
                del self.__sub_categories_dict[category_name]

    def __projects_in_main_pages(self, size_projects_in_line, num_categories_in_main_page):
        root = etree.parse(StringIO(get(self.__absolute_url).text), etree.HTMLParser()).getroot()
        team_selection_element = root.find(".//div[@class='mainbannerslider']")
        team_selection_projects_list = team_selection_element.findall(".//div[@class='mainbanner_inner_image']")
        for project in team_selection_projects_list:
            self.__projects_in_main_page_dict['Team-selection'].append(project.find(".//div[@class='desc']/b").text)

        temp_list = []
        [temp_list.append([]) for _ in range(num_categories_in_main_page)]
        projects_in_main_page = root.findall(".//a[@class='projectthumb']/div[@class='desc']/b")
        for idx, a in enumerate(projects_in_main_page[:-size_projects_in_line]):
            temp_list[idx // size_projects_in_line].append(a.text)

        for idx, projects_list in enumerate(temp_list):
            self.__projects_in_main_page_dict[self.__projects_in_main_page_indexes_dict[str(idx + 1)]] = projects_list

    @staticmethod
    def __save_projects_url(projects_url):
        with open('debug.html', mode='w', encoding='utf-8') as file:
            file.write(get(projects_url).text)

    def __get_projects_detail(self, project_url, category_name, sub_category_name):
        project_detail_dict, id_category, total_pages = {}, 0, 0
        logging.info("\n{}".format(project_url))
        root = etree.parse(StringIO(get(project_url).text), etree.HTMLParser()).getroot()
        id_category = root.find(".//div[@class='catprojcontent']/input[@id='idpage']").attrib['value']
        total_pages = int(root.find(".//div[@class='catprojcontent']/input[@id='totalpages']").attrib['value'])

        for page_idx in range(1, total_pages + 1):
            url = self.__absolute_url + '/' + self.__get_projects_page.format(id_category, page_idx)
            root = etree.parse(StringIO(get(url).text), etree.HTMLParser()).getroot()
            root = root.find(".//table")
            for element in root.findall(".//td[@valign='top']"):
                date_now = str(datetime.now()).split('.')[0]
                temp_dict = {'Category': category_name, 'Sub-category': sub_category_name, 'Date': date_now,
                             'Project-name': element.find(".//div[@class='desc']/b").text.strip()}
                if not self.__is_live_projects_flag and temp_dict['Project-name'] in self.__project_names_list:
                    continue
                for main_category_name in self.__projects_in_main_page_indexes_reverse_dict.keys():
                    temp_dict[main_category_name] = False
                    temp_dict[main_category_name + '-IDX'] = -1
                for projects_in_main_page_category in self.__projects_in_main_page_dict.keys():
                    if temp_dict['Project-name'] in self.__projects_in_main_page_dict[projects_in_main_page_category]:
                        temp_dict[projects_in_main_page_category] = True
                        temp_dict[projects_in_main_page_category + '-IDX'] = self.__projects_in_main_page_dict[
                            projects_in_main_page_category].index(temp_dict['Project-name'])
                        break
                cash_details = element.findall(".//div[@class='cash']")
                temp_dict['Funding-goal'] = ''.join(element.find(".//div[@class='target']/span/b").itertext())
                temp_dict['Funding-percentage'] = cash_details[0].find(".//b").text.replace('%', '')
                temp_dict['Funding-days'] = cash_details[1].find(".//b").text
                temp_dict['URL'] = self.__absolute_url + '/' + element.find(".//a[@class='"
                                                                            "projectthumb']").attrib['href']
                temp_dict['Location'] = ''.join(element.find(".//div[@class='l']").itertext())
                temp_dict = dict([(key, str(value).strip()) for key, value in temp_dict.items()])
                if not self.__is_break_between_runs and temp_dict['Project-name'] not in self.__project_names_list:
                    temp_dict['From-Start'] = True
                else:
                    temp_dict['From-Start'] = False
                project_detail_dict[element.find(".//div[@class='desc']/b").text.strip()] = temp_dict

            if SLEEPING_FLAG:
                sleeping_time = uniform(3, 9)
                logging.info("\nSleeping ZzZzZ..., time={0:.2f}s".format(sleeping_time))
                sleep(uniform(3, 9))

        return project_detail_dict

    def __save_projects_data_as_pickle_zip(self, temp_df=None):
        if temp_df is None:
            temp_df = self.__project_data_frame
        with open(self.__csv_pickle_path, mode='wb') as pickle_file:
            dump(temp_df, pickle_file)

    def __unsaved_projects_file(self):
        with open(self.__csv_pickle_path.split('_')[0] + ' unsaved projects.txt', 'w') as f:
            for url in self.__projects_not_saved_list:
                f.write(url + '\n')

    def run(self):
        """
            Scanning all the projects on the site by category.
            Once here, extract all the information and save it as a CSV file.
        """
        projects_details_dict, projects_details_list = {}, []
        if self.__is_live_projects_flag:
            if path.isfile('projects_url_details.pickle') and DEBUG:
                with open('projects_url_details.pickle', mode='rb') as fp:
                    projects_details_dict = dict(load(fp))
                for category_name in projects_details_dict.keys():
                    for sub_category_name in projects_details_dict[category_name].keys():
                        for project_dict in projects_details_dict[category_name][sub_category_name].values():
                            is_error_flag, project_data = HeadStartProject(
                                project_dict, self.__images_and_videos_folder_path, self.__absolute_url,
                                self.__csv_headers_list, self.__reward_size).get_project_data()
                            if is_error_flag:
                                logging.debug("\nThe project can not be saved in this url ({})".format(project_data))
                                self.__projects_not_saved_list.append(project_data)
                            else:
                                projects_details_list.append(project_data)

                            if SLEEPING_FLAG:
                                sleeping_time = uniform(3, 9)
                                logging.info("\nSleeping ZzZzZ..., time={0:.2f}s".format(sleeping_time))
                                sleep(uniform(3, 9))
            else:
                for category_name in self.__sub_categories_dict.keys():
                    if DEBUG:
                        category_name = 'אמנות'
                    projects_details_dict[category_name] = {}
                    for sub_category_name in self.__sub_categories_dict[category_name]:
                        if DEBUG:
                            sub_category_name = 'כתיבה'
                        category_projects_url = \
                            self.__absolute_url + '/' + self.__sub_categories_dict[category_name][sub_category_name]

                        projects_details_dict[category_name][sub_category_name] = \
                            self.__get_projects_detail(category_projects_url, category_name, sub_category_name)
                        for project_dict in projects_details_dict[category_name][sub_category_name].values():
                            is_error_flag, project_data = HeadStartProject(
                                project_dict, self.__images_and_videos_folder_path, self.__absolute_url,
                                self.__csv_headers_list, self.__reward_size).get_project_data()
                            if is_error_flag:
                                logging.debug("\nThe project can not be saved in this url ({})".format(project_data))
                                self.__projects_not_saved_list.append(project_data)
                            else:
                                projects_details_list.append(project_data)

                            if SLEEPING_FLAG:
                                sleeping_time = uniform(3, 9)
                                logging.info("\nSleeping ZzZzZ..., time={0:.2f}s".format(sleeping_time))
                                sleep(uniform(3, 9))

                        if DEBUG:
                            break
                    if DEBUG:
                        break
                if not DEBUG:
                    with open('projects_url_details.pickle', mode='wb') as fp:
                        dump(projects_details_dict, fp)
                    logging.info("\nThe file in '{}' path was successfully created"
                                 .format('projects_url_details.pickle'))

        else:
            projects_details_dict = self.__get_projects_detail(ENDING_UP_PROJECTS_URL, 'empty', 'empty')
            for project_idx, project_dict in enumerate(projects_details_dict.values()):
                is_error, project_data = HeadStartProject(
                    project_dict, self.__images_and_videos_folder_path, self.__absolute_url,
                    self.__csv_headers_list, self.__reward_size, False).get_project_data()
                if is_error:
                    logging.debug("\nThe project can not be saved in this url ({})".format(project_data))
                    self.__projects_not_saved_list.append(project_data)
                else:
                    projects_details_list.append(project_data)

                if project_idx % 50 == 0:
                    temp_df = DataFrame(projects_details_list, columns=self.__csv_headers_list)
                    self.__save_projects_data_as_pickle_zip(temp_df)

                if not DEBUG:
                    sleeping_time = uniform(3, 9)
                    logging.info("\nsleeping ZzZzZ..., time={0:.2f}s, Date=".format(sleeping_time), str(datetime.now()))
                    sleep(uniform(3, 9))

        self.__unsaved_projects_file()
        temp_df = DataFrame(projects_details_list, columns=self.__csv_headers_list)
        self.__project_data_frame = concat([self.__project_data_frame, temp_df])
        if 'Popular-IDX' in self.__csv_headers_list:
            self.__csv_headers_list.remove('Popular-IDX')
        copy_csv_headers_list = copy(self.__csv_headers_list)
        copy_csv_headers_list.remove('Date')
        self.__project_data_frame = DataFrame(self.__project_data_frame).drop_duplicates(copy_csv_headers_list)
        if self.__is_live_projects_flag:
            self.__project_data_frame = self.__project_data_frame.sort_values(by=['Project-name', 'Date'],
                                                                              ascending=[True, True])
        else:
            self.__project_data_frame = self.__project_data_frame.sort_values(by=['Date', 'Project-name'],
                                                                              ascending=[False, True])
        self.__save_projects_data_as_pickle_zip()
        csv_path = self.__csv_pickle_path.split('.')[0] + '.csv'
        self.__project_data_frame = self.__project_data_frame.to_csv(csv_path, index=False)


def __is_break_between_runs():
    if path.isfile(PROGRAM_LOG_PATH):
        with open(PROGRAM_LOG_PATH) as f:
            last_log_message_date = str(f.readlines()[-2:-1]).split('\\t')[0].replace("['", '')
        last_error_date = datetime.strptime(last_log_message_date, '%d/%m/%Y %H:%M:%S')
        now_date = datetime.strptime(datetime.now().strftime('%d/%m/%Y %H:%M:%S'), '%d/%m/%Y %H:%M:%S')
        return not 0 <= abs((now_date - last_error_date).days) <= 1

    return False


def main():
    """
        Start the main program.
        Scan all projects on the site and extract the information from each project and save it as a CSV file.
        In addition, saving all project images and videos.
    """
    logger.setting_up_logger('debug', 'info', PROGRAM_LOG_PATH)
    logging.debug("\nStart running the program")
    images_and_videos_folder_path = 'images_and_videos'
    live_csv_headers_list = ['Date', 'Project-name', 'URL', 'Project-owner', 'Project-text', 'Funding-goal',
                             'Funding-raised', 'Funding-percentage', 'Updates', 'Backers', 'Responses', 'Funding-days']
    for idx in range(1, REWARD_NUMS):
        live_csv_headers_list.append('Reward-price-' + str(idx))
        live_csv_headers_list.append('Reward-text-' + str(idx))
        live_csv_headers_list.append('Reward-backers-' + str(idx))

    finished_csv_headers_list = copy(live_csv_headers_list)
    live_csv_headers_list += ['Rewards-num', 'Youtube-num', 'Location', 'Category', 'Sub-category', 'Images-num',
                              'ID-txt', 'Team-selection', 'Team-selection-IDX', 'About_to_end', 'About_to_end-IDX',
                              'Popular', 'Popular-IDX', 'New', 'New-IDX', 'Partner', 'Partner-URL', 'Partner-Name',
                              'Partner_Total-Funding', 'Partner_Project-Numbers', 'From-Start']
    finished_csv_headers_list += ['Rewards-num', 'Youtube-num', 'Images-num', 'ID-txt', 'Partner',
                                  'Partner-URL', 'Partner-Name', 'Partner_Total-Funding', 'Partner_Project-Numbers',
                                  'From-Start']

    if not path.isdir(images_and_videos_folder_path):
        makedirs(images_and_videos_folder_path)
        logging.debug("\nThe '{}' folder was created successfully".format(images_and_videos_folder_path))

    if not path.isfile(LIVE_PROJECT_PICKLE_PATH):
        with open(LIVE_PROJECT_PICKLE_PATH, mode='wb') as pickle_file:
            dump(DataFrame(columns=live_csv_headers_list), pickle_file)
        logging.debug("\nThe '{}' pickle file was created successfully".format(LIVE_PROJECT_PICKLE_PATH))

    if not path.isfile(FINISHED_PROJECT_PICKLE_PATH):
        with open(FINISHED_PROJECT_PICKLE_PATH, mode='wb') as pickle_file:
            dump(DataFrame(columns=finished_csv_headers_list), pickle_file)
        logging.debug("\nThe '{}' pickle file was created successfully".format(FINISHED_PROJECT_PICKLE_PATH))

    try:
        while True:
            start_time = clock()
            if not LIVE_PROJECTS_FLAG:
                ProjectsURL(URL, images_and_videos_folder_path, finished_csv_headers_list,
                            FINISHED_PROJECT_PICKLE_PATH, 30, False, live_projects_flag=False).run()
                logging.info("\nFinished projects: The total time taken to take out all the information is: {} mins"
                             "".format((clock() - start_time) / 60))
                break

            else:
                break_between_runs_flag = __is_break_between_runs()
                ProjectsURL(URL, images_and_videos_folder_path, live_csv_headers_list, LIVE_PROJECT_PICKLE_PATH, 30,
                            break_between_runs_flag).run()

            logging.info("\nLive projects: The total time taken to take out all the information is: {:.4f} mins"
                         "".format((clock() - start_time) / 60))
            # Pause the script for 2 hours and then rescan the projects.
            two_hour = 2 * 60 * 60
            sleeping_time = uniform(two_hour, 1.5 * two_hour)
            logging.info("\nSleeping ZzZzZ..., time={:.2f}h".format(sleeping_time / (60 * 60)))
            break
            sleep(sleeping_time)
    except Exception as _:
        logger.change_logger_file(PROGRAM_LOG_PATH, ERROR_LOG_PATH)
        logging.debug("\Error: {}".format(print_exc))


if __name__ == '__main__':
    main()
