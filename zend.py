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

tzu = tzutc()

url_api = f'https://{config.zend_subdomain}.zendesk.com/api/v2/'
url_posts = url_api + 'community/posts.json?sort_by=updated_at&include=topics'
url_post_comments = url_api + 'community/posts/{post_id}/comments.json'
url_post = url_api + 'community/posts/{post_id}.json'
url_articles = url_api + \
    'help_center/incremental/articles.json?start_time={start_time}\
    &include=sections'
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


class Article():
    def __init__(self, json_data, sections_dict):
        self.id = json_data['id']
        self.title = json_data['title']
        self.updated_at = parse(json_data['updated_at'])
        self.created_at = parse(json_data['created_at'])
        self.html_url = json_data['html_url']
        self.section = sections_dict[json_data['section_id']]


class Post():
    def __init__(self, json_data, topics_dict):
        self.id = json_data['id']
        self.title = json_data['title']
        self.updated_at = parse(json_data['updated_at'])
        self.created_at = parse(json_data['created_at'])
        self.html_url = json_data['html_url']
        self.topic = topics_dict[json_data['topic_id']]


def get_start_date():
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


def get_z_objects(api_url, cls, start_date):
    cls_name = cls.__name__
    data_objects = fetch_data(api_url)
    if data_objects['count'] == 0:
        objects = []
    else:
        if cls is Article:
            sections_dict = {s['id']: s['name']
                             for s in data_objects['sections']}
            objects = [Article(a, sections_dict)
                       for a in data_objects['articles']]
        elif cls is Post:
            topics_dict = {t['id']: t['name'] for t in data_objects['topics']}
            objects = [Post(p, topics_dict) for p in data_objects['posts']
                       if parse(p['updated_at']) > start_date]

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


if __name__ == '__main__':
    html_articles = html_articles_new_comm = \
        html_posts_new = html_posts_new_comm = None

    start_date = get_start_date()
    start_date_unix = int(time.mktime(start_date.timetuple()))
    logging.info(f'Start date: {start_date}')

    articles_new, articles_with_new_comm = get_z_objects(
        url_articles.format(start_time=start_date_unix), Article, start_date)

    if articles_new and len(articles_new) > 0:
        html_articles = \
            '<b>Обновления в статьях:</b>\n\n' + '\n\n'\.join(
                [f'[{a.section}]\n{a.title}\
                 <a href="{a.html_url}">\nЧитать статью</a>'
                 for a in articles_new])

    if articles_with_new_comm and len(articles_with_new_comm) > 0:
        html_articles_new_comm =\
            '<b>Cтатьи с новыми комментариями:</b>\n\n' + '\n\n'\
            .join([f'[{a.section}]\n{a.title}\
                  <a href="{a.html_url}">\nЧитать статью</a>'
                  for a in articles_with_new_comm])

    posts_new, posts_with_new_comm = get_z_objects(url_posts, Post, start_date)

    if posts_new and len(posts_new) > 0:
        html_posts_new = \
            '<b>Новые посты:</b>\n\n' + '\n\n'.join(
                [f'[{p.topic}]\n{p.title}\
                 <a href="{p.html_url}">\nЧитать пост</a>'
                 for p in posts_new])

    if posts_with_new_comm and len(posts_with_new_comm) > 0:
        html_posts_new_comm = \
            '<b>Посты с новыми комментариями:</b>\n\n' + '\n\n'.join(
                [f'[{p.topic}]\n{p.title}\
                 <a href="{p.html_url}">\nЧитать пост</a>'
                 for p in posts_with_new_comm])

    send_to_telegram(html_articles, html_articles_new_comm,
                     html_posts_new, html_posts_new_comm)

    upd_start_date(datetime.now().astimezone(tzu))
