from config import (
    db_config,
    path_keyword_url_folder,
    path_keyword_list_folder,
    site_domain,
    keyword_url_separator,
    idyurl_path_location
)
from helper import chunker
from orator import DatabaseManager
import os
import glob
import time

class Main:
    db = None
    keyword_list = []
    result_list = []
    idyurl_list = []
    
    def __init__(self):
        self.db_config = db_config
        self.path_input = path_keyword_list_folder
        self.path_output = path_keyword_url_folder
        self.read_all_keywords(self.path_input)

    def get_connect(self):
        if not self.db:
            self.db = DatabaseManager(self.db_config)
            return self.db
        return self.db

    def read_all_keywords(self, input_folder_path):
        directory_path = input_folder_path
        text_files = glob.glob(os.path.join(directory_path, '*.txt'))
        for file_path in text_files:
            with open(file_path, 'r', encoding='utf-8') as file:
                self.keyword_list = file.readlines()

    def write_to_path_id_yurl(self):
        php_str = 'var arr_pages = [' + ', '.join(['"' + url + '"' for url in self.idyurl_list]) + '];'
        with open(idyurl_path_location, 'w') as f:
            f.write(php_str)

    def write_to_path_file(self):
        filename = '{}.txt'.format(str(time.time()).split('.')[0])
        file_path = '{}{}'.format(self.path_output, filename)
        if not os.path.exists(file_path):
            open(file_path, 'x').close()

        with open(file_path, 'w', encoding='utf-8') as file:
            for item in self.result_list:
                file.write(str(item).strip() + '\n')

    def generate_url(self, keyword, cate_id, article_id):
        result = '{}{}{}{}/{}.html'.format(keyword.strip(), keyword_url_separator, site_domain, cate_id, article_id)
        # result = '{}{}{}{}/{}.html'.format(keyword, keyword_url_separator, site_domain, 'post', article_id)
        result_idurl = '/{}/{}.html'.format(cate_id, article_id)

        print('>>{}'.format(result))

        self.result_list.append(result)
        self.idyurl_list.append(result_idurl)

    def execute_all(self):
        for keywords in chunker(self.keyword_list, 100):
            for keyword in keywords:
                print('\nsearch for keyword {}....'.format(keyword))
                if not keyword.strip():
                    continue

                for results in self.get_connect().table('zbp_post') \
                    .join('zbp_category', 'zbp_post.log_CateID', '=', 'zbp_category.cate_ID') \
                    .where('zbp_post.log_Status', 0) \
                    .where(
                        self.get_connect().query().where('zbp_post.log_Title', 'like', '%{}%'.format(keyword.strip())) \
                        .or_where('zbp_post.log_Content', 'like', '%{}%'.format(keyword.strip()))
                    ) \
                    .select('zbp_post.log_ID', 'zbp_category.cate_Alias') \
                    .chunk(100):
                    for result in results:
                        self.generate_url(keyword, result['cate_Alias'], result['log_ID'])

        self.write_to_path_file()
        self.write_to_path_id_yurl()


if __name__ == '__main__':
    main = Main()
    main.execute_all()
