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

url_api = f'https://{config.zend_subdomain}.zendesk.com/api/v2/'
url_posts = url_api + 'community/posts.json?sort_by=updated_at&include=topics'
url_post_comments = url_api + 'community/posts/{post_id}/comments.json'
url_post = url_api + 'community/posts/{post_id}.json'
url_articles = url_api + (
    'help_center/incremental/articles.json?start_time={start_time}'
    '&include=sections')
url_article_comments = url_api + \
    'help_center/ru/articles/{article_id}/comments.json'

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
            logging.FileHandler('zend.log'),
            logging.StreamHandler()
    ])


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
    def __init__(self, json_data, section):
        super().__init__(json_data)
        self.section = section

    def repr_html(self):
        return f'[{self.section}]\n{self.title}\
                <a href="{self.html_url}">\nЧитать статью</a>'


class Post(ZendObject):
    def __init__(self, json_data, topic):
        super().__init__(json_data)
        self.topic = topic

    def repr_html(self):
        return f'[{self.topic}]\n{self.title}\
                <a href="{self.html_url}">\nЧитать пост</a>'


def get_start_date():
    tzu = tzutc()
    with shelve.open(config.shelve_name) as db:
        start_date = db.get(
            'start_date', datetime.now().astimezone(tzu) - timedelta(days=7))
    return start_date


def upd_start_date(new_date):
    with shelve.open(config.shelve_name) as db:
        db['start_date'] = new_date


def fetch_data(url):
    response = requests.get(url, auth=(config.zend_user, config.zend_pwd))
    if response.status_code != 200:
        logging.error(
            f'Response status: {response.status_code}. \
            Problem with the request. Exiting')
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


def get_zend_objects(api_url, cls, start_date):
    cls_name = cls.__name__
    json_data = fetch_data(api_url)
    if json_data is None or json_data['count'] == 0:
        objects = []
    else:
        if cls is Article:
            objects = create_articles_from_json(json_data)
        elif cls is Post:
            objects = create_posts_from_json(json_data, start_date)

    logging.info(f'{cls_name}s, fetched count: {len(objects)}')

    objects_new = [o for o in objects if o.created_at >= start_date]
    objects_upd = [o for o in objects if o.created_at < start_date]

    logging.info(f'{cls_name}s, new articles count: {len(objects_new)}')
    logging.info(f'{cls_name}s, updated articles count: {len(objects_upd)}')

    objects_with_new_comm = []
    for o in objects_upd:
        if cls is Article:
            data_comments = fetch_data(
                url_article_comments.format(article_id=o.id))
        elif cls is Post:
            data_comments = fetch_data(url_post_comments.format(post_id=o.id))

        if any(parse(comm['updated_at']) > start_date
                for comm in data_comments['comments']):
                    objects_with_new_comm.append(o)

    logging.info(
        f'{cls_name}s, with new comments count: {len(objects_with_new_comm)}')

    return (objects_new, objects_with_new_comm)


def create_html_block(title, elements):
    return f'[<b>{title}:</b>\n\n' + '\n\n'.join(
        [elem.repr_html() for elem in elements])

if __name__ == '__main__':
    html_articles = html_articles_new_comm = \
        html_posts_new = html_posts_new_comm = None

    start_date = get_start_date()
    timestamp = int(start_date.timestamp())
    logging.info(f'Start date: {start_date}, timestamp: {timestamp}')

    articles_new, articles_with_new_comm = get_zend_objects(
        url_articles.format(start_time=timestamp), Article, start_date)

    if articles_new:
        html_articles = create_html_block('Обновления в статьях', articles_new)

    if articles_with_new_comm:
        html_articles_new_comm = create_html_block(
            'Cтатьи с новыми комментариями',
            articles_with_new_comm)

    posts_new, posts_with_new_comm = get_zend_objects(
        url_posts, Post, start_date)

    if posts_new:
        html_posts_new = create_html_block('Новые посты', posts_new)

    if posts_with_new_comm:
        html_posts_new_comm = create_html_block(
            'Посты с новыми комментариями',
            posts_with_new_comm)

    send_to_telegram(html_articles, html_articles_new_comm,
                     html_posts_new, html_posts_new_comm)

    upd_start_date(datetime.now().astimezone(tzu))
