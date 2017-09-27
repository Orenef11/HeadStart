# coding=utf-8
from io import StringIO
from urllib import request
from lxml import etree
from requests import get
from os import path, makedirs
from bs4 import BeautifulSoup
from subprocess import call, check_output
from shutil import move
from datetime import datetime
import logging


class HeadStartProject(object):
    """"
        Retrieve the information for each project by values in csv_headers
    """
    def __init__(self, project_details_dict, images_and_videos_folder_path, absolute_url, csv_headers_list,
                 reward_size, is_new_projects_flag=True):
        self.__project_data = {**dict(project_details_dict), **dict([(key, 'empty') for key in csv_headers_list
                                                                     if key not in project_details_dict.keys()])}
        self.__root = etree.parse(StringIO(get(project_details_dict['URL']).text), etree.HTMLParser()).getroot()
        self.__soup_root = BeautifulSoup(get(self.__project_data['URL']).content, 'lxml')
        self.__images_list = []
        self.__user_agent = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                                            ' Chrome/36.0.1941.0 Safari/537.36')]
        self.__absolute_url = absolute_url + '/'
        self.__images_and_videos_folder_path = images_and_videos_folder_path
        self.__csv_headers_list = csv_headers_list
        self.__replace_bad_chars = ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '.']
        self.__reward_size = reward_size
        self.__youtube_url = 'https://www.youtube.com/watch?v={}'
        self.__is_new_projects_flag = is_new_projects_flag

    def get_project_data(self):
        """

        :return:
        """
        youtube_links = self.__soup_root.find_all({'iframe': 'src'})
        youtube_links = [link.attrs['src'].split('?')[0].split('/')[-1] for link in youtube_links
                         if 'youtube' in link.attrs['src']]
        youtube_links = [self.__youtube_url.format(link) for link in youtube_links]

        self.__project_data['Youtube-num'] = len(youtube_links)
        if self.__is_new_projects_flag:
            self.__download_youtube_video(youtube_links)
        self.__project_data['Project-owner'] = \
            self.__root.find(".//div[@class='by1']/span[@class='by2']").text.strip()
        idx = 1
        reward_elements_list = self.__root.findall(".//div[@class='projectwindowbottom']/"
                                                   "div[@class='payoption off off-dis']")
        reward_elements_list += self.__root.findall(".//div[@class='projectwindowbottom']/a[@class='payoption']")

        self.__project_data['Rewards-num'] = len(reward_elements_list)
        logging.info("\nRetrieving information from {}".format(self.__project_data['URL']))
        for payoption in reward_elements_list:

            self.__project_data['Reward-price-' + str(idx)] = \
                payoption.find(".//span[@style='font-size:18px;']/b[@style='font-size:45px; font-weight:bold;']").text
            self.__project_data['Reward-text-' + str(idx)] = payoption.find(".//div[@class='payoptc']/b").text
            temp = '\n'.join(payoption.find(
                ".//div[@style='font-size: 12px; line-height: 17px; margin-top:10px']").itertext())
            self.__project_data['Reward-text-' + str(idx)] = temp.strip()
            self.__project_data['Reward-backers-' + str(idx)] = \
                ''.join(payoption.find(".//div[@class='optionline']").itertext()).strip().split(' ')
            self.__project_data['Reward-backers-' + str(idx)] = self.__project_data['Reward-backers-' + str(idx)][0]
            idx += 1

        try:
            self.__project_data['Funding-raised'] = self.__root.find(".//div[@class='cu']").text.strip()
        except Exception as _:
            return True, self.__project_data['URL']

        self.__project_data['comments_size'] = ''

        for element in self.__root.findall(".//div[@class='inn']"):
            if element.text == 'עידכונים':
                updates_size = element.find('.//span')
                if updates_size is not None:
                    self.__project_data['Updates'] = updates_size.text
                else:
                    self.__project_data['Updates'] = '0'
            elif element.text == 'תומכים':
                backers_size = element.find('.//span')
                if backers_size is not None:
                    self.__project_data['Backers'] = backers_size.text
                else:
                    self.__project_data['Backers'] = '0'
            elif element.text == 'תגובות':
                responses_size = element.find('.//span')
                if responses_size is not None:
                    self.__project_data['Responses'] = responses_size.text
                else:
                    self.__project_data['Responses'] = '0'
        if not self.__is_new_projects_flag:
            if int(self.__project_data['Responses']) == 0:
                return True, self.__project_data['URL']
            else:
                comment_url = self.__absolute_url + '/projectcomments.aspx?id={}'.format(
                    self.__project_data['URL'].split('?id=')[-1])
                self.__project_data['Date'] = self.__find_date(comment_url)
        self.__project_data['Project-text'] = ''
        for element in self.__root.findall(".//div[@class='descclass']/p"):
            text = ' '.join(element.itertext()).strip()
            if text != '':
                self.__project_data['Project-text'] += '\n' + text
        self.__project_data['ID-txt'] = ''.join(
            self.__root.find(".//div[@class='bottom']/div[@class='description']").itertext()).strip()
        self.__project_data['ID-txt'] = self.__project_data['ID-txt'].replace('\r', '')
        self.__project_data['ID-txt'] = ' '.join([text.strip() for text in self.__project_data['ID-txt'].split('\n')
                                                  if text.strip() != ''])
        self.__project_data['ID-txt'] = '.\n'.join([text.strip() for text in self.__project_data['ID-txt'].split('. ')])
        images_element = self.__root.findall(".//div[@class='descclass']/figure/img")

        partner_element = self.__root.find(".//div[@class='projectpane']/div[@class='partner-panel-box']")
        if partner_element is not None:
            partner_url = partner_element.find('.//a')
            if partner_url is not None:
                partner_url = self.__absolute_url + partner_url.attrib['href']
                self.__project_data['Partner'] = True
                self.__project_data['Partner-URL'] = partner_url
                partner_root = etree.parse(StringIO(get(partner_url).text), etree.HTMLParser()).getroot()
                partner_details = partner_root.find(".//div[@class='partner-side']")
                self.__project_data['Partner-Name'] = partner_details.find(".//h1").text

                partner_total_funding = partner_details.find(".//h4/span")
                if partner_total_funding is not None:
                    self.__project_data['Partner_Total-Funding'] = partner_total_funding.text.strip()
                    if '₪' in self.__project_data['Partner_Total-Funding']:
                        self.__project_data['Partner_Total-Funding'] = \
                            self.__project_data['Partner_Total-Funding'].replace('₪', '')

                partner_project_numbers = partner_details.findall(".//h4/span")
                if partner_project_numbers is not None and len(partner_project_numbers) >= 2:
                    self.__project_data['Partner_Project-Numbers'] = partner_project_numbers[1].text.strip()

        images_url_list = []
        for idx, element in enumerate(images_element):
            url = self.__absolute_url + element.attrib['src']
            url = url.replace('\\', '/')
            images_url_list.append(url)
        if self.__is_new_projects_flag:
            self.__download_image(images_url_list)
        self.__project_data['Images-num'] = len(images_element)

        self.__project_data = dict([(key, str(value)) for key, value in self.__project_data.items()])
        self.__project_data = dict([(key, value.strip()) for key, value in self.__project_data.items()])

        return False, [self.__project_data[header] for header in self.__csv_headers_list]

    @staticmethod
    def __find_date(comment_url):
        root = etree.parse(StringIO(get(comment_url).text), etree.HTMLParser()).getroot()
        comments_list = root.findall(".//div[@class='projectcomment']")[-1]
        first_comment = comments_list.find(".//li")
        date_list = first_comment.attrib['title'].split(":")[-1].split('/')
        return datetime(int(date_list[2]), int(date_list[1]), int(date_list[0]))

    def __download_youtube_video(self, youtube_links):
        for youtube_link in youtube_links:
            try:
                # command = str("youtube-dl --audio-format best {}".format(l))
                command = "youtube-dl --get-filename {}".format(youtube_link)
                project_name = self.__project_data['Project-name']
                for bad_char in self.__replace_bad_chars:
                    project_name = project_name.replace(bad_char, '')
                folder_path = path.join(self.__images_and_videos_folder_path, project_name)
                if not path.isdir(folder_path):
                    makedirs(folder_path)
                file_name = check_output(command, shell=True).decode().replace('\n', '')
                file_path = path.join(folder_path, file_name)
                if not path.isfile(file_path):
                    # command = str("youtube-dl --audio-format best {}".format(l))
                    command = str("youtube-dl {}".format(youtube_link))
                    call(command.split())
                    move(file_name, file_path)
            except Exception as e:
                logging.info("\nYoutube Error: {}\nYoutube-URL {}\nError message {}".format(
                    self.__project_data["Project-name"], youtube_link, str(e)))
                continue

    def __download_image(self, images_url_list):
        for img_url in images_url_list:
            try:
                if len(img_url.split('https://')) != 2:
                    img_url = 'https://' + img_url.split('https://')[-1]
                project_name = self.__project_data['Project-name']
                for bad_char in self.__replace_bad_chars:
                    project_name = project_name.replace(bad_char, '')
                folder_path = path.join(self.__images_and_videos_folder_path, project_name)

                if not path.isdir(folder_path):
                    makedirs(folder_path)
                opener = request.build_opener()
                opener.addheaders = self.__user_agent
                request.install_opener(opener)
                img_path = path.join(folder_path, img_url.split('/')[-1])
                if not path.isfile(img_path):
                    request.urlretrieve(img_url, img_path)
            except Exception as e:
                logging.info("\nImage Error: Projects name {}\n Image URL {}\nError message {}"
                             .format(self.__project_data["Project-name"], img_url, str(e)))
                logging.info("\Image Error: Projects name {}\n Image URL {}\nError message {}")


    @staticmethod
    def __find_all_data_in_element(element, tag):
        all_data = ""
        for node in element:
            node = node.findall(tag)
            for element in node:
                data = "".join(element.itertext()).strip()
                if data != "":
                    all_data = all_data + "\r\n" + data

        all_data = all_data.replace("None", "")
        return all_data

# Debug
# HeadStartProject({'URL': 'https://www.headstart.co.il/project.aspx?id=24693'}, 'images_and_videos', 'https://www.headstart.co.il', [], 30).get_project_data()
