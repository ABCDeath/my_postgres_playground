import argparse
import datetime
import discogs_client
import json
import pickle
import psycopg2
import requests
import vk_requests
import time
from bs4 import BeautifulSoup


def wait_request_timing(prev_time, rate):
    if time.time() - prev_time < 1 / rate:
        time.sleep(1 / rate - (time.time() - prev_time))


class VkAudioGetter(object):
    def __init__(self, credentials):
        self._query_prev_time = 0
        try:
            with open('session', 'rb') as storage:
                cookies = pickle.load(storage)
                self._session = requests.Session()
                self._session.cookies.update(cookies)
        except FileNotFoundError:
            # получаем страничку логина, если куки не нашлись
            self._session = requests.Session()
            self._session.headers.update({
                'User-agent': 'Mozilla/5.0 (Windows NT 6.1; rv:52.0) '
                              'Gecko/20100101 Firefox/52.0'
            })

            resp = self._session.get('https://vk.com')
            resp_soup = BeautifulSoup(resp.text, 'lxml')
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
            self._session.post('https://login.vk.com', answer)
            # TODO: check status
            with open('session', 'wb') as storage:
                pickle.dump(self._session.cookies, storage)

    def user_audio_list(self, user_id):
        url = ''.join(['https://m.vk.com/audios', user_id])
        _QUERY_RATE = 0.5
        self._query_prev_time = 0

        artists = []
        offset = 0
        last_audio = None
        while True:
            wait_request_timing(self._query_prev_time, _QUERY_RATE)

            resp = self._session.get(url, params={'offset': offset},
                                     allow_redirects=False)
            resp.raise_for_status()

            audio_soup = BeautifulSoup(resp.text, 'lxml')
            container = audio_soup(
                'div', class_='audios_block audios_list _si_container')

            if not container:
                return artists

            # следующий запрос возвращает не ту аудиозапись,
            # которая ожидалась, даже если offset будет постоянным
            # (например, 50) или равен количеству ранее выданных аудиозаписей,
            # поэтому запоминаем, на чем остановились.
            container_start = 0
            if last_audio is not None:
                for num, item in enumerate(
                        container[0].find_all('span', 'ai_title')):
                    if item.string == last_audio:
                        container_start = num + 1
                        break

            last_audio = container[0].find_all('span', 'ai_title')[-1].string
            content = container[0].find_all('span', 'ai_artist')

            artists.extend([a.string for a in content[container_start:]])

            offset += len(content)


class AudioDB(object):
    def __init__(self, user, password):
        self._db_conn = psycopg2.connect(
            host='localhost', port='5432', user=user, password=password,
            database='music')

        # в бд таблицы жанров, поджанров, исполнителей, пользователей и
        # таблица с исполнителями у пользователей, в которой есть количество
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
                genre_id INT NOT NULL REFERENCES genre (id),
                subgenre_id INT NOT NULL REFERENCES subgenre (id)
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

        with self._db_conn.cursor() as cursor:
            for item in query:
                cursor.execute(item)

        self._db_conn.commit()

    def _add_genre(self, genre):
        with self._db_conn.cursor() as cursor:
            cursor.execute(
                'INSERT INTO genre (name) VALUES (%s) ON CONFLICT DO NOTHING',
                (genre if genre else 'None',))
        self._db_conn.commit()

    def _add_subgenre(self, subgenre):
        query = """
            INSERT INTO subgenre (name) VALUES (%s) ON CONFLICT DO NOTHING
            """
        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (subgenre if subgenre else 'None',))
        self._db_conn.commit()

    def add_user(self, vk_id, name):
        with self._db_conn.cursor() as cursor:
            query = """
                INSERT INTO vk_user (vk_id, name)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
                """
            cursor.execute(query, (vk_id, name))
        self._db_conn.commit()

    def update_user_artists(self, user_vk_id, artists):
        artists = [x.lower() for x in artists]

        cursor = self._db_conn.cursor()

        cursor.execute(
            'SELECT id FROM vk_user WHERE vk_user.vk_id = %s', (user_vk_id,))
        user_id = cursor.fetchall()[0][0]

        # для начала убрать исполнителей, которых пользователь уже удалил
        query = """
            SELECT artist.name FROM artist
            WHERE EXISTS (
                    SELECT artist_id FROM user_to_artist u2a
                    WHERE u2a.artist_id = artist.id AND user_id = %s
                )
            """
        cursor.execute(query, (user_id,))
        irrelevant = list(set([x[0] for x in cursor.fetchall()]).difference(
            set([x for x in artists])))

        if irrelevant:
            # вместо JOIN в DELETE используется USING
            query = """
                DELETE FROM user_to_artist
                USING artist
                WHERE user_id = %s AND 
                    user_to_artist.artist_id = artist.id AND
                    artist.name IN (SELECT * FROM unnest(%s))
                """
            cursor.execute(query, (user_id, irrelevant))

        for artist in set(artists):
            # пробуем сначала запись обновить, потому что INSERT ON CONFLICT
            # работает только с уникальными значениями,
            # которых в этой таблице нет
            query = """
                UPDATE user_to_artist SET tracks_num = %(tracks_num)s
                WHERE user_id = %(user_id)s AND
                    EXISTS (
                        SELECT 1 FROM artist
                        WHERE id = user_to_artist.artist_id AND
                            artist.name = %(artist)s
                    )
                """
            cursor.execute(query, {
                'tracks_num': artists.count(artist),
                'user_id': user_id,
                'artist': artist
            })

            # если ничего не обновилось, то записи нет -- создаём
            if not cursor.rowcount:
                query = """
                    INSERT INTO user_to_artist(user_id, artist_id, tracks_num)
                    VALUES(
                        %(user_id)s,
                        (SELECT id FROM artist WHERE artist.name = %(artist)s),
                        %(tracks_num)s
                    )
                    """
                cursor.execute(query, {
                    'user_id': user_id,
                    'artist': artist.lower(),
                    'tracks_num': artists.count(artist)
                })

        cursor.close()
        self._db_conn.commit()

    def add_genre(self, genre, subgenre):
        self._add_genre(genre)
        self._add_subgenre(subgenre)

    def add_artist(self, name, genre, subgenre):
        if genre is None:
            subgenre = None

        # TODO: для genre вместо None -- unknown, для sub -- ''

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

        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (name.lower(),
                                   genre if genre else 'None',
                                   subgenre if subgenre else 'None'))
        self._db_conn.commit()

    def get_artist_genre(self, artist_name):
        query = """
            SELECT subgenre.name, genre.name FROM artist
            INNER JOIN genre ON genre.id = artist.genre_id
            INNER JOIN subgenre ON subgenre.id = artist.subgenre_id
            WHERE artist.name = %s
            """

        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (artist_name.lower(),))
            resp = cursor.fetchall()

        return resp

    def is_artist_exists(self, artist_name):
        with self._db_conn.cursor() as cursor:
            cursor.execute('SELECT name FROM artist WHERE artist.name = %s',
                           (artist_name.lower(),))
            resp = cursor.fetchall()

        return resp

    def get_user_artists(self, vk_id, limit=None):
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

        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (vk_id, limit))
            resp = cursor.fetchall()

        return resp

    def get_user_genres(self, vk_id, sub=False, limit=None):
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
                
                WHERE g.name != 'None' AND
                    EXISTS (
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
                    SELECT artist.genre_id AS gid, SUM(tracks_num) AS sum FROM user_to_artist u2a
                    INNER JOIN artist ON artist.id = u2a.artist_id
                    WHERE EXISTS (
                        SELECT 1 FROM vk_user u
                        WHERE u.id = u2a.user_id AND u.vk_id = %s
                    )
                    GROUP BY artist.genre_id
                ) AS u_tracks ON u_tracks.gid = genre.id
                WHERE genre.name != 'None'
                ORDER BY
                    sum DESC,
                    genre.name ASC
                LIMIT %s
                """

        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (vk_id, limit))
            resp = cursor.fetchall()

        return resp

    def users_genre_intersection(self, vk_id_user_1, vk_id_user_2, limit=None):
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
            
            WHERE genre.name != 'None'
            ORDER BY
                min_sum DESC,
                genre.name ASC
            LIMIT %s
            """

        with self._db_conn.cursor() as cursor:
            cursor.execute(query, (vk_id_user_1, vk_id_user_2, limit))
            resp = cursor.fetchall()

        return resp

class GenreSearcher(object):
    _USER_AGENT = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/61.0.3163.100 Safari/537.36'
    }
    _QUERY_RATE = 1  # per second

    def __init__(self, musicbrainz_credentials):
        self._google_query_prev_time = 0
        self._musicbrainz_query_prev_time = 0
        self._musicbrainz_cred = {
            'app': musicbrainz_credentials['app_name'],
            'token': musicbrainz_credentials['token']
        }

    def _encode(self, string):
        return string.replace('#', '%23').replace(' ', r'%20')

    def _discogs(self, artist_name):
        client = discogs_client.Client(
            'MyGetArtistGenreApp/0.1',
            user_token='bgXSArBadKChVYdHHRnfylRtPmXLZfUDZyRQKjzB')
        res = client.search(artist_name, type='artist')
        # нет жанров/тегов/стилей у исполнителя, только по релизам
        pass

    def _google(self, artist_name):
        # TODO: попробовать вместо гугла какой-нибудь duckduckgo
        query = artist_name + ' genre'

        wait_request_timing(self._google_query_prev_time, self._QUERY_RATE)

        escaped_query = query.replace(' ', '+')
        url = 'https://www.google.com/search?q={}&num={}&hl={}'.format(
            escaped_query, 1, 'en')
        response = requests.get(url, headers=self._USER_AGENT)

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
        url = 'https://musicbrainz.org/ws/2/artist/' \
              '?query=artist:{}&inc=tags&fmt=json'.format(
            self._encode(artist_name))

        wait_request_timing(self._musicbrainz_query_prev_time, self._QUERY_RATE)

        resp = requests.get(url, headers=self._USER_AGENT)

        self._musicbrainz_query_prev_time = time.time()

        for artist in resp.json()['artists']:
            if artist['name'].lower() == artist_name.lower():
                if 'tags' not in artist:
                    return None

                tag_name = sorted(artist['tags'], key=lambda tag: tag['count'],
                                  reverse=True)[0]['name']
                tags = tag_name.split(' ')[::-1]
                return tags[0], (tags[1] if len(tags) > 1 else None)
        else:
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


if __name__ == '__main__':
    start_time = datetime.datetime.now()

    argparser = argparse.ArgumentParser()
    argparser.add_argument('ids', type=str, nargs='+')
    users_id = argparser.parse_args()

    with open('credentials.json', 'r') as cred_file:
        credentials = json.load(cred_file)

    db = AudioDB(credentials['postgres']['user'],
                 credentials['postgres']['password'])
    search = GenreSearcher(credentials['musicbrainz'])
    api = vk_requests.create_api(service_token=credentials['vk']['token'])
    vk_audio_get = VkAudioGetter(credentials['vk'])
    del credentials

    print('+{}: получение аудиозаписей.'.format(time_diff(start_time)))

    for user_id in users_id.ids:
        user_info = api.users.get(user_ids=int(user_id), lang=0)[0]
        db.add_user(user_id, ' '.join(
            [user_info['first_name'], user_info['last_name']]))

        print('+{}: пользователь {} {}, id: {}'.format(
            time_diff(start_time), user_info['first_name'],
            user_info['last_name'], user_id))

        # get audio list
        artists = vk_audio_get.user_audio_list(user_id)

        print('+{}: {} аудиозаписей, поиск и добавление тегов в бд'.format(
            time_diff(start_time), len(artists)))

        for artist in artists:
            if db.is_artist_exists(artist):
                continue

            genre, subgenre = search.search(artist)
            db.add_artist(artist, genre, subgenre)

        print('+{}: добавление исполнителей к пользователям'.format(
            time_diff(start_time)))

        db.update_user_artists(user_id, artists)

    for user_id in users_id.ids:
        print('+{}: {}:'.format(time_diff(start_time), user_id),
              db.get_user_artists(user_id, 5))
        print('+{}: {}:'.format(time_diff(start_time), user_id),
              db.get_user_genres(user_id, True, 5))

    print('+{}: {} - {}: {}'.format(
        time_diff(start_time), users_id.ids[0], users_id.ids[1],
        db.users_genre_intersection(users_id.ids[0], users_id.ids[1], 5)))
