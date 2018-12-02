# encoding: utf-8
"""
Скрипт логинится в вконтакте и обычными запросами с мобильной версии сайта
достаёт список исполнителей из аудиозаписей пользователей, так как в открытом
api больше нет возможности работать с аудиозаписями.
Далее для каждого исполнителя, если его ещё нет в базе данных, находится через
api ресурса с информацией о треках и исполнителях musicbrainz
или простым запросом в гугле (если находится) музыкальное направление.
Исполнители и жанры добавляются в локальную бд postgresql.
После этого можно посмотреть различную информацию о музыкальных предпочтениях
уже добавленных в базу пользователей.
"""

import asyncio
import argparse
import datetime
import json
import logging
import requests
import time

import aiohttp
import asyncpg
import vk_requests
from bs4 import BeautifulSoup


def wait_request_timing(prev_time, rate):
    if time.time() - prev_time < 1 / rate:
        time.sleep(1 / rate - (time.time() - prev_time))


class VkAudioGetter:
    _headers = {
        'User-agent': 'Mozilla/5.0 (Windows NT 6.1; rv:52.0) '
                      'Gecko/20100101 Firefox/52.0'
    }

    # TODO: как реализованы методы __enter__ и __exit__ для async/awit?

    async def open(self, credentials):
        self._session = aiohttp.ClientSession(headers=self._headers,
                                              raise_for_status=True)

        try:
            self._session.cookie_jar.load('session')

            logging.info('куки загружены')

            return
        except FileNotFoundError:
            pass

        logging.info('не удалось открыть файл с куками, логинимся заново')

        # получаем страничку логина, если куки не нашлись
        async with self._session.get('https://vk.com') as r:
            resp = await r.text()

        resp_soup = BeautifulSoup(resp, 'lxml')

        answer = {
            '_origin': 'https://vk.com',
            'act': 'login',
            'email': credentials['login'],
            'pass': credentials['password'],
            'ip_h': resp_soup('input', {'name': 'ip_h'})[0]['value'],
            'lg_h': resp_soup('input', {'name': 'lg_h'})[0]['value'],
            'role': 'al_frame'
        }

        # логинимся, ip_h и lg_h, похоже, csrf-токен
        async with self._session.post('https://login.vk.com', data=answer) as r:
            await r.text()

        logging.info('залогинились')

        self._session.cookie_jar.save('session')

    async def close(self):
        await self._session.close()

    async def get_audio_list(self, user_id, queue, finish):
        url = f'https://m.vk.com/audios{user_id}'
        query_rate = 0.5

        offset = 0
        last_audio = None

        while True:
            logging.info('получаем аудиозаписи id: %s, offset: %s',
                         user_id, offset)

            async with self._session.get(
                    url, params={'offset': offset}, allow_redirects=False) as r:
                logging.info('status: %s', r.status)
                resp = await r.text()

            audio_soup = BeautifulSoup(resp, 'lxml')
            container = audio_soup(
                'div', class_='audios_block audios_list _si_container')

            if not container:
                break

            titles = container[0].find_all('span', 'ai_title')

            # следующий запрос возвращает не ту аудиозапись,
            # которая ожидалась, даже если offset будет постоянным
            # (например, 50) или равен количеству ранее выданных аудиозаписей,
            # поэтому запоминаем, на чем остановились.
            container_start = (titles.index(last_audio)
                               if last_audio and last_audio in titles else 0)

            last_audio = titles[-1]
            content = container[0].find_all('span', 'ai_artist')

            queue.put_nowait((
                user_id,
                [a.string.lower() for a in content[container_start:]]))

            offset += len(content)

            logging.info('ждём %s секунд', query_rate)

            await asyncio.sleep(query_rate)

        logging.info('закончили получать список аудио')


class AudioDB:
    def __init__(self):
        self._db_conn = None

    async def open(self, user, password):
        self._db_conn = await asyncpg.connect(
            host='localhost', port='5432', user=user, password=password,
            database='music')

        # в бд таблицы жанров, поджанров, исполнителей, пользователей и
        # таблица исполнителей к пользователям, в которой есть количество
        # треков исполнителя

        query = (
            """
            CREATE TABLE IF NOT EXISTS genre (
                id serial PRIMARY KEY,
                name VARCHAR(64) NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS subgenre (
                id serial PRIMARY KEY,
                name VARCHAR(64) NOT NULL UNIQUE
            )
            """,

            """
            CREATE TABLE IF NOT EXISTS artist (
                id serial PRIMARY KEY,
                name VARCHAR(128) NOT NULL UNIQUE DEFAULT 'unknown',
                genre_id INT REFERENCES genre (id),
                subgenre_id INT REFERENCES subgenre (id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS vk_user (
                id serial PRIMARY KEY,
                vk_id VARCHAR(8) NOT NULL UNIQUE,
                name VARCHAR(64) NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS user_to_artist (
                user_id INT NOT NULL REFERENCES vk_user (id),
                artist_id INT NOT NULL REFERENCES artist (id),
                tracks_num INT NOT NULL
            )
            """
        )

        async with self._db_conn.transaction():
            for item in query:
                await self._db_conn.execute(item)

    async def close(self):
        await self._db_conn.close()

    async def _add_genre(self, genre):
        query = 'INSERT INTO genre (name) VALUES ($1) ON CONFLICT DO NOTHING'

        async with self._db_conn.transaction():
            await self._db_conn.executemany(query, genre)

    async def _add_subgenre(self, subgenre):
        query = """
            INSERT INTO subgenre (name) VALUES ($1) ON CONFLICT DO NOTHING
            """

        async with self._db_conn.transaction():
            await self._db_conn.executemany(query, subgenre)

    async def add_user(self, user_data):
        query = """
            INSERT INTO vk_user (vk_id, name)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
            """

        async with self._db_conn.transaction():
            await self._db_conn.executemany(query, user_data)

    async def update_user_artists(self, vk_id, artists):
        async with self._db_conn.transaction():
            await self._db_conn.execute(
                'SELECT id FROM vk_user WHERE vk_user.vk_id = $1', (vk_id,))
            db_user_id = await self._db_conn.fetchrow()[0]

            # для начала убрать исполнителей, которых пользователь уже удалил
            query = """
                DELETE FROM user_to_artist u2a
                USING artist
                WHERE user_id = %s AND 
                    u2a.artist_id = artist.id AND
                    artist.name <> ALL(%s)
                """
            await self._db_conn.execute(query, (db_user_id, list(set(artists))))

            query = """
                SELECT name FROM artist
                WHERE name = ANY(%s) AND EXISTS (
                    SELECT 1 FROM user_to_artist u2a
                    WHERE user_id = %s AND u2a.artist_id = artist.id
                    )
                """
            await self._db_conn.execute(query, (list(set(artists)), db_user_id))
            res = await self._db_conn.fetch()
            existing = set(x[0] for x in res)

            to_insert = [(db_user_id, a, artists.count(a))
                         for a in (set(artists) - existing)]
            if to_insert:
                query = """
                    INSERT INTO user_to_artist(user_id, artist_id, tracks_num)
                    VALUES(
                        %s,
                        (SELECT id FROM artist WHERE artist.name = %s),
                        %s
                    )
                    """
                await self._db_conn.executemany(query, to_insert)

            to_update = [(artists.count(a), db_user_id, a) for a in existing]
            if to_update:
                query = """
                    UPDATE user_to_artist SET tracks_num = %s
                    WHERE user_id = %s AND
                        EXISTS (
                            SELECT 1 FROM artist
                            WHERE id = user_to_artist.artist_id AND
                                artist.name = %s
                        )
                    """
                await self._db_conn.executemany(query, to_update)

    def add_genre(self, genre, subgenre):
        self._add_genre(list(set(genre)))
        self._add_subgenre(list(set(subgenre)))

    async def add_artist(self, artists_data):
        genre = [(d[1],) for d in artists_data if d[1]]
        subgenre = [(d[2],) for d in artists_data if d[2]]

        self.add_genre(genre, subgenre)

        query = """
            INSERT INTO artist (name, genre_id, subgenre_id)
            VALUES (
                %s,
                (SELECT id FROM genre WHERE name = %s),
                (SELECT id FROM subgenre WHERE name = %s)
            )
            ON CONFLICT DO NOTHING
            """

        async with self._db_conn.transaction():
            await self._db_conn.executemany(query, artists_data)

    async def get_artist_genre(self, artist_name):
        query = """
            SELECT subgenre.name, genre.name FROM artist
            LEFT JOIN genre ON genre.id = artist.genre_id
            LEFT JOIN subgenre ON subgenre.id = artist.subgenre_id
            WHERE artist.name = %s
            """

        await self._db_conn.execute(query, (artist_name,))
        return await self._db_conn.fetch()

    async def get_existing(self, artists):
        query = 'SELECT name FROM artist WHERE artist.name = ANY(%s)'

        await self._db_conn.execute(query, (artists,))
        return await self._db_conn.fetch()

    async def get_user_artists(self, vk_id, limit=None):
        query = """
            SELECT artist.name, u2a.tracks_num FROM artist
            INNER JOIN (
                SELECT artist_id, tracks_num FROM user_to_artist
                WHERE EXISTS (
                    SELECT 1 FROM vk_user u
                    WHERE
                        u.id = user_to_artist.user_id AND
                        u.vk_id = %s
                )
            ) AS u2a ON u2a.artist_id = artist.id
            ORDER BY
                u2a.tracks_num DESC,
                artist.name ASC
            LIMIT %s
            """

        await self._db_conn.execute(query, (vk_id, limit))
        return await self._db_conn.fetch()

    async def get_user_genres(self, vk_id, sub=False, limit=None):
        if sub:
            query = """
                SELECT s.name, g.name, SUM(tracks_num) FROM user_to_artist u2a
                
                INNER JOIN (
                    SELECT id, genre_id AS gid, subgenre_id AS subid FROM artist
                ) AS a ON a.id = u2a.artist_id
                
                INNER JOIN (
                    SELECT id, name FROM genre
                ) AS g ON g.id = a.gid
                
                INNER JOIN (
                    SELECT id, name FROM subgenre
                ) AS s ON s.id = a.subid
                
                WHERE EXISTS (
                        SELECT 1 FROM vk_user u
                        WHERE u.id = u2a.user_id AND u.vk_id = %s
                    )
                
                GROUP BY g.name, s.name
                ORDER BY
                    SUM(tracks_num) DESC,
                    g.name ASC
                LIMIT %s
                """
        else:
            query = """
                SELECT genre.name, sum FROM genre
                INNER JOIN (
                    SELECT artist.genre_id AS gid, SUM(tracks_num) AS sum
                    FROM user_to_artist u2a
                    INNER JOIN artist ON artist.id = u2a.artist_id
                    WHERE EXISTS (
                        SELECT 1 FROM vk_user u
                        WHERE u.id = u2a.user_id AND u.vk_id = %s
                    )
                    GROUP BY artist.genre_id
                ) AS u_tracks ON u_tracks.gid = genre.id
                ORDER BY
                    sum DESC,
                    genre.name ASC
                LIMIT %s
                """

        await self._db_conn.execute(query, (vk_id, limit))
        return await self._db_conn.fetch()

    async def users_genre_intersection(self, vk_id_user_1, vk_id_user_2, limit=None):
        query = """
            SELECT genre.name,
                LEAST(sum_user_1, sum_user_2) AS min_sum FROM genre
            
            INNER JOIN (
                SELECT SUM(tracks_num) AS sum_user_1,
                    artist.genre_id AS a_gid_user_1 FROM user_to_artist
                INNER JOIN artist ON artist.id = artist_id
                WHERE
                    EXISTS (
                        SELECT * FROM vk_user u
                        WHERE u.id = user_to_artist.user_id AND u.vk_id = %s
                    )
                GROUP BY artist.genre_id
            ) AS join_user_1 ON join_user_1.a_gid_user_1 = genre.id
            
            INNER JOIN (
                SELECT SUM(tracks_num) AS sum_user_2,
                    artist.genre_id AS a_gid_user_2 FROM user_to_artist
                INNER JOIN artist ON artist.id = artist_id
                WHERE
                    EXISTS (
                        SELECT * FROM vk_user u
                        WHERE u.id = user_to_artist.user_id AND u.vk_id = %s
                    )
                GROUP BY artist.genre_id
            ) AS join_user_2 ON join_user_2.a_gid_user_2 = genre.id
            
            ORDER BY
                min_sum DESC,
                genre.name ASC
            LIMIT %s
            """

        await self._db_conn.execute(query, (vk_id_user_1, vk_id_user_2, limit))
        return await self._db_conn.fetch()


class GenreSearcher:
    _user_agent = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/61.0.3163.100 Safari/537.36'
    }
    _query_rate = 1

    def __init__(self, musicbrainz_credentials):
        self._google_query_prev_time = 0
        self._musicbrainz_query_prev_time = 0
        self._musicbrainz_cred = {
            'app': musicbrainz_credentials['app_name'],
            'token': musicbrainz_credentials['token']
        }

    @staticmethod
    def _prepare(string):
        return string.replace('#', '%23').replace(' ', r'%20')

    def _google(self, artist_name):
        query = ''.join([artist_name, ' genre']).replace(' ', '+')

        wait_request_timing(self._google_query_prev_time, self._query_rate)

        url = f'https://www.google.com/search?q={query}&num=1&hl=en'
        response = requests.get(url, headers=self._user_agent)

        self._google_query_prev_time = time.time()

        soup = BeautifulSoup(response.text, 'lxml')

        # рандомные названия классов в html, поэтому такая фигня
        main = soup('a', class_='rl_item rl_item_base')
        alternative = soup('div', class_='kp-hc')

        if main:
            genre_tag = main[0].find('div', class_='title')
            genre = genre_tag.string.lower().split(' ')[::-1]
        elif alternative:
            genre_tag = alternative[0].find('div', role='heading').next
            genre = genre_tag.string.lower().split(' ')[::-1]
        else:
            return None

        return genre[0], (genre[1] if len(genre) > 1 else None)

    def _musicbrainz(self, artist_name):
        # TODO: запрашивать сразу список, из него выбирать
        url = f'https://musicbrainz.org/ws/2/artist/' \
              f'?query=artist:{self._prepare(artist_name)}&inc=tags&fmt=json'

        wait_request_timing(self._musicbrainz_query_prev_time, self._query_rate)

        resp = requests.get(url, headers=self._user_agent)

        self._musicbrainz_query_prev_time = time.time()

        for artist in resp.json()['artists']:
            if (artist['name'].lower() != artist_name.lower() or
                    'tags' not in artist):
                return None

            tag_name = sorted(artist['tags'], key=lambda tag: tag['count'],
                              reverse=True)[0]['name']
            tags = tag_name.split(' ')[::-1]

            return tags[0], (tags[1] if len(tags) > 1 else None)

        return None

    def search(self, artist_name):
        res = self._musicbrainz(artist_name)
        if res is None:
            res = self._google(artist_name)
            if res is None:
                res = None, None
        return res


def time_diff(start_time):
    return str(datetime.datetime.now() - start_time)


async def vk_get_audio(vk_id, queue, credentials):
    vk_audio_getter = VkAudioGetter()
    await vk_audio_getter.open(credentials)

    for id in vk_id:
        await vk_audio_getter.get_audio_list(id, queue)

    await vk_audio_getter.close()
    queue.put_nowait(None)


class dbTasks:
    def __init__(self, vk_artists_queue, to_search, from_search):
        self._db = AudioDB()
        self._vk_artists_queue = vk_artists_queue
        self._to_search = to_search
        self._from_search = from_search
        self._artists_to_update = asyncio.Queue()
        self._update_event = asyncio.Event()

    async def open(self, credentials):
        await self._db.open(credentials['user'], credentials['password'])

    async def close(self):
        await self._db.close()

    async def get_existing(self):
        while True:
            logging.info('ждём данных из вк в очереди')

            new_part = await self._vk_artists_queue.get()
            self._vk_artists_queue.task_done()

            if not new_part:
                logging.info('данных нет, завершение задачи')
                break

            self._artists_to_update.put_nowait(new_part)

            new_part = new_part[1]

            existing = await self._db.get_existing(set(new_part))
            need_search = set(new_part) - set(existing)

            logging.info(f'нужно найти теги для {len(need_search)} '
                         f'исполнителей из {len(set(new_part))} '
                         f'({len(new_part)} всего)')

            if need_search:
                self._to_search.put_nowate(need_search)
            else:
                self._update_event.set()

        self._to_search.put_nowate([])

    async def update_artist(self):
        while True:
            logging.info('ждём данных в очереди с тегами')

            new_part = await self._from_search.get()
            self._from_search.task_done()

            if not new_part:
                logging.info('данных нет, завершение задачи')
                break

            logging.info(f'добавляем {len(new_part)} исполнителей с тегами')

            await self._db.add_artist(new_part)

            self._update_event.set()

    async def update_user(self):
        while True:
            logging.info('ждём event')
            await self._update_event.wait()

            logging.info('достаём пользователя и исполнителей из очередей')

            new_part = await self._artists_to_update.get()

            if not new_part:
                logging.info('обновлять нечего, завершение задачи')
                break

            user_id = new_part[0]
            user = api.users.get(user_ids=int(user_id), lang=0)[0]
            name = ' '.join([user['first_name'], user['last_name']])

            logging.info(f'добавляем пользователя {user_id}: {name}')

            await self._db.add_user(([user_id, name],))

            new_part = new_part[1]

            logging.info(f'добавляем {len(new_part)} исполнителей к {user_id}')

            await self._db.update_user_artists(user_id, new_part)


def run_tasks(vk_id, credentials):
    loop = asyncio.get_event_loop()
    vk_audio_queue = asyncio.Queue()

    loop.run_until_complete(asyncio.gather(
            vk_get_audio(vk_id, vk_audio_queue, credentials['vk']),
        ))
    loop.close()


if __name__ == '__main__':
    start_time = datetime.datetime.now()

    argparser = argparse.ArgumentParser()
    argparser.add_argument('ids', type=str, nargs='+')
    argparser.add_argument('-v', '--verbose', action='store_true')

    args = argparser.parse_args()

    logging.basicConfig(
        format='+%(relativeCreated)d - %(funcName)s: %(levelname)s: %(message)s',
        level=logging.INFO if args.verbose else logging.DEBUG)

    logging.info('список аудиозаписей для id: %s', args.ids)

    with open('credentials.json') as cred_file:
        credentials = json.load(cred_file)

    run_tasks(args.ids, credentials)

    db = AudioDB(credentials['postgres']['user'],
                 credentials['postgres']['password'])

    search = GenreSearcher(credentials['musicbrainz'])

    api = vk_requests.create_api(service_token=credentials['vk']['token'])

    vk_audio_get = VkAudioGetter(credentials['vk'])

    del credentials

    print(f'+{time_diff(start_time)}: получение аудиозаписей.')

    for user_id in args.ids:
        user_info = api.users.get(user_ids=int(user_id), lang=0)[0]

        db.add_user(user_id, ' '.join(
            [user_info['first_name'], user_info['last_name']]))

        print(f'+{time_diff(start_time)}: '
              f'пользователь {user_info["first_name"]} '
              f'{user_info["last_name"]}, id: {user_id}')

        artists = vk_audio_get.get_audio_list(user_id)

        print(f'+{time_diff(start_time)}: {len(artists)} аудиозаписей, '
              f'поиск и добавление тегов в бд')

        for artist in artists:
            if db.is_artist_exists(artist):
                continue

            genre, subgenre = search.search(artist)
            db.add_artist(artist, genre, subgenre)

        print(f'+{time_diff(start_time)}: добавление исполнителей')

        db.update_user_artists(user_id, artists)

    for user_id in args.ids:
        print(f'+{time_diff(start_time)}: {user_id}:',
              db.get_user_artists(user_id, 5))
        print(f'+{time_diff(start_time)}: {user_id}:',
              db.get_user_genres(user_id, True, 5))

    print(f'+{time_diff(start_time)}: {args.ids[0]} - {args.ids[1]}:',
          db.users_genre_intersection(args.ids[0], args.ids[1], 5))
