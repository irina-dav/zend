import logging
import shelve
import time
from datetime import datetime, timedelta

import pytz
import requests
from dateutil.parser import parse
from dateutil.tz import tzlocal, tzutc
from telegram import ParseMode
from telegram.error import TimedOut
from telegram.ext import Updater

import config

URL_API = f'https://{config.zend_subdomain}.zendesk.com/api/v2/'

tzu = tzutc()

logging.basicConfig(**config.log_config)


class ZendObject():
    def __init__(self, json_data):
        self.id = json_data['id']
        self.title = json_data['title']
        self.updated_at = parse(json_data['updated_at'])
        self.created_at = parse(json_data['created_at'])
        self.html_url = json_data['html_url']

    def __repr__(self):
        return f'{self.title} - {self.html_url}'


class Article(ZendObject):
    title_new = 'Обновления в статьях'
    title_new_comments = 'Cтатьи с новыми комментариями'

    @property
    def url_comments(self):
        return URL_API + f'help_center/ru/articles/{self.id}/comments.json'

    @classmethod
    def get_url_objects(cls, timestamp=None):
        return URL_API + \
            f'help_center/incremental/articles.json?start_time={timestamp}' \
            '&include=sections'

    def __init__(self, json_data, section):
        super().__init__(json_data)
        self.section = section

    def repr_html(self):
        return f'[{self.section}]\n{self.title}\
                <a href="{self.html_url}">\nЧитать статью</a>'


class Post(ZendObject):
    title_new = 'Новые посты'
    title_new_comments = 'Посты с новыми комментариями'

    @property
    def url_comments(self):
        return URL_API + f'community/posts/{self.id}/comments.json'

    @classmethod
    def get_url_objects(cls, timestamp=None):
        return URL_API + \
            'community/posts.json?sort_by=updated_at&include=topics'

    def __init__(self, json_data, topic):
        super().__init__(json_data)
        self.topic = topic

    def repr_html(self):
        return f'[{self.topic}]\n{self.title}\
            <a href="{self.html_url}">\nЧитать пост</a>'


def get_start_date():
    with shelve.open(config.shelve_name) as db:
        start_date = db.get(
            'start_date', datetime.now().astimezone(tzu) - timedelta(days=7))
    return start_date


def upd_start_date(new_date):
    with shelve.open(config.shelve_name) as db:
        db['start_date'] = new_date


def fetch_url(url):
    response = requests.get(url, auth=(config.zend_user, config.zend_pwd))
    if response.status_code != 200:
        logging.error(f'Response status: {response.status_code}. Exiting')
        return None
    return response.json()


def send_to_telegram(*messages):
    try:
        updater = Updater(token=config.token,
                          request_kwargs=config.bot_request_args)
        bot = updater.bot
        message = '\n\n'.join([m for m in messages if m])
        if message:
            bot.sendMessage(chat_id=config.channel_id,
                            text=message,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True)
    except Exception as ex:
        logging.error(ex)
        raise ex


def create_articles_from_json(json_data):
    sections_dict = {s['id']: s['name'] for s in json_data['sections']}
    return [Article(a, sections_dict[a['section_id']])
            for a in json_data['articles']]


def create_posts_from_json(json_data, start_date):
    topics_dict = {t['id']: t['name'] for t in json_data['topics']}
    return [Post(p, topics_dict[p['topic_id']]) for p in json_data['posts']
            if parse(p['updated_at']) > start_date]


def create_objects_from_json(json_data, start_date):
    if json_data['count'] == 0:
        return []
    if 'articles' in json_data:
        return create_articles_from_json(json_data)
    elif 'posts' in json_data:
        return create_posts_from_json(json_data, start_date)
    else:
        raise Exception('Wrong format of json data')


def get_objects(api_url, start_date):
    logging.info(f'Fetch url {api_url}')
    json_data = fetch_url(api_url)

    objects = create_objects_from_json(json_data, start_date)
    logging.info(f'Fetched objects count: {len(objects)}')

    objects_new = [o for o in objects if o.created_at >= start_date]
    logging.info(f'New objects count: {len(objects_new)}')

    objects_upd = [o for o in objects if o.created_at < start_date]
    logging.info(f'Updated objects count: {len(objects_upd)}')

    return (objects_new, objects_upd)


def get_new_comments(objects_upd, start_date):
    objects_with_new_comm = []
    for o in objects_upd:
        json_comments = fetch_url(o.url_comments)
        if any(parse(comm['updated_at']) > start_date
                for comm in json_comments['comments']):
            objects_with_new_comm.append(o)

    logging.info(f'With new comments count: {len(objects_with_new_comm)}')

    return objects_with_new_comm


def search_updates(cls, start_date):
    html_new = html_new_comm = None

    timestamp = int(start_date.timestamp())
    logging.info(f'Start date: {start_date}, timestamp: {timestamp}')

    objects_new, objects_upd = get_objects(
        cls.get_url_objects(timestamp), start_date)
    objects_with_new_comm = []
    if objects_upd:
        objects_with_new_comm = get_new_comments(objects_upd, start_date)

    if objects_new:
        html_new = format_html_block(cls.title_new, objects_new)

    if objects_with_new_comm:
        html_new_comm = format_html_block(
            cls.title_new_comments, objects_with_new_comm)

    return (html_new, html_new_comm)


def format_html_block(title, elements):
    return f'[<b>{title}:</b>\n\n' + '\n\n'.join(
        [elem.repr_html() for elem in elements])

if __name__ == '__main__':
    html_articles = html_articles_new_comm = \
        html_posts_new = html_posts_new_comm = None

    start_date = get_start_date()
    html_articles, html_articles_new_comm = search_updates(Article, start_date)
    html_posts_new, html_posts_new_comm = search_updates(Post, start_date)

    print(html_articles, html_articles_new_comm,
          html_posts_new, html_posts_new_comm)
    # send_to_telegram(html_articles, html_articles_new_comm,
    #                 html_posts_new, html_posts_new_comm)

    upd_start_date(datetime.now().astimezone(tzu))
