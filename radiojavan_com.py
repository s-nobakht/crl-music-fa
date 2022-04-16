#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re

import mysql.connector
from mysql.connector import Error
import hashlib
from configparser import ConfigParser
from requests import Session
import datetime
import os, shutil
import utilities as ut
import sys

import logging
#logging.basicConfig(level=logging.DEBUG)

class RadioJavan(object):
    def __init__(self, configFile='config.ini'):
        self.config = ConfigParser()
        self.config.read(configFile)

        import logging

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # create a file handler
        handler = logging.FileHandler(self.config.get('output', 'songsUpdateLog'))
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logFormatter = logging.Formatter('%(levelname)s - %(message)s')

        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        self.logger.addHandler(consoleHandler)

        # add the handlers to the logger
        self.logger.addHandler(handler)

        self.logger.info('Program Started !')


        """ Connect to MySQL database """
        try:
            self.conn = mysql.connector.connect(host=self.config.get('database', 'host'),
                                                database=self.config.get('database', 'name'),
                                                user=self.config.get('database', 'user'),
                                                password=self.config.get('database', 'password'))
            if self.conn.is_connected():
                self.logger.info('Connected to MySQL database')
        except Error as e:
            self.logger.error(e)
            if self.config.getint('mode', 'stopInException'):
                sys.exit(0)


    def crawlSingers(self):
        cursor = self.conn.cursor()
        imageDownloadList = open(self.config.get('output', 'singersImages') +
                                 self.config.get('output', 'singersImagesDownloadListFile'), "w")

        f = open(self.config.get('output', 'singersPagesFile'), 'w')  # singer page address list
        singerDuplicateCounter = 0
        singerPattern = r'<li>.*?href="\/mp3s\/browse\/artist\/(.*?)">.*?\/static\/mp3\/(.*?)\-photo_240x180.jpg".*?class="artist_name">(.*?)<\/span>.*?<\/li>'

        query = "INSERT INTO rj_singers(hash_id, english_name,farsi_name,link,image_link) VALUES(%s,%s,%s,%s,%s)" \
                " ON DUPLICATE KEY UPDATE hash_id=%s,english_name=%s,farsi_name=%s,link=%s,image_link=%s"

        singerData = {'id': '', 'hash_id': '', 'name': '', 'link': '', 'songs': ''}
        songInfo = {'id': '', 'name': '', 'lyric': ''}
        singersHashList = []

        # program works in offline mode (good for debug)
        if self.config.getint('mode', 'offline'):
            with open(self.config.get('mode', 'offlineSingerListSampleFile'), 'r') as myFile:
                sampleText = myFile.read()

            singers = re.findall(singerPattern, sampleText, re.S | re.I)

            if singers:
                for singerInfo in singers:
                    singerData['id'] = cursor.lastrowid
                    singerData['name'] = singerInfo[2]
                    singerData['link'] = singerInfo[0]
                    singerData['image_link'] = singerInfo[1]
                    hashString = singerData['link'] + self.config.get('general', 'hashIdBaseString')
                    singerData['hash_id'] = hashlib.md5(hashString.encode('utf-8')).hexdigest()
                    if singerData['hash_id'] not in singersHashList:
                        args = (singerData['hash_id'], singerData['name'], singerData['name'], singerData['link'],
                                singerData['image_link'],
                                singerData['hash_id'], singerData['name'], singerData['name'], singerData['link'],
                                singerData['image_link'])
                        try:
                            # Execute the SQL command
                            cursor.execute(query, args)
                            # Commit your changes in the database
                            self.conn.commit()
                            print("%s Added !" % (singerData['name']))

                            imageDownloadList.write('%s%s.jpg\n\tdir=%s\n\tout=%s.jpg\n' %
                                                    (self.config.get('rjavan', 'baseImageURL'),
                                                     singerData['image_link'],
                                                     self.config.get('output', 'singersImages'),
                                                     singerData['hash_id']))
                            imageDownloadList.write('%s%s%s.jpg\n\tdir=%s\n\tout=%s%s.jpg\n' %
                                                    (self.config.get('rjavan', 'baseImageURL'),
                                                     singerData['image_link'],
                                                     self.config.get('rjavan', 'imageResizePostfix'),
                                                     self.config.get('output', 'singersImages'),
                                                     singerData['hash_id'],
                                                     self.config.get('rjavan', 'imageResizePostfix')
                                                     ))
                            singersHashList.append(singerData['hash_id'])
                        except:
                            # Rollback in case there is any error
                            self.conn.rollback()
                    else:
                        print("% Already Exist !" % (singerData['name']))
                        singerDuplicateCounter += 1

                    if singerDuplicateCounter >= self.config.getint('rjavan', 'maxDuplicatedSinger'):
                        break

                    f.write(singerInfo[0] + "\r\n")
            else:
                print("nothing Found")

            cursor.close()
            # self.conn.close()
            f.close()
            imageDownloadList.close()
            # f = open('singerListSample.html', 'w') #opens file with name of "test.txt"
            # f.write(response.text)
            # f.close()
            # print (response.text)


        # program works in online mode (real operational mode)
        else:
            print("Online Mode !")
            session = Session()
            # if proxy is enabled
            if self.config.get('network', 'proxy'):
                proxies = {'https': self.config.get('network', 'proxy')}
                session.proxies = proxies
                # HEAD requests ask for *just* the headers, which is all you need to grab the
                # session cookie

            session.head(self.config.get('rjavan', 'baseURL'))

            for pageNumber in range(1, self.config.getint('rjavan', 'maxSingersPages')):
                response = session.get(
                    url=self.config.get('rjavan', 'baseSingerBrowseURL'),
                    data={
                        'page': pageNumber,
                    },
                    headers={
                        'User-Agent': self.config.get('network', 'userAgent'),
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Referer': self.config.get('rjavan', 'baseURL')
                    }
                )
                sampleText = response.text

                singers = re.findall(singerPattern, sampleText, re.S | re.I)

                if singers:
                    for singerInfo in singers:
                        singerData['id'] = cursor.lastrowid
                        singerData['name'] = singerInfo[2]
                        singerData['link'] = singerInfo[0]
                        singerData['image_link'] = singerInfo[1]
                        hashString = singerData['link'] + self.config.get('general', 'hashIdBaseString')
                        singerData['hash_id'] = hashlib.md5(hashString.encode('utf-8')).hexdigest()
                        if singerData['hash_id'] not in singersHashList:
                            args = (singerData['hash_id'], singerData['name'], singerData['name'], singerData['link'],
                                    singerData['image_link'],
                                    singerData['hash_id'], singerData['name'], singerData['name'], singerData['link'],
                                    singerData['image_link'])
                            try:
                                # Execute the SQL command
                                cursor.execute(query, args)
                                # Commit your changes in the database
                                self.conn.commit()
                                print("%s Added !" % (singerData['name'].replace('%', '%%')))

                                imageDownloadList.write('%s%s.jpg\n\tdir=%s\n\tout=%s.jpg\n' %
                                                        (self.config.get('rjavan', 'baseImageURL'),
                                                         singerData['image_link'],
                                                         self.config.get('output', 'singersImages'),
                                                         singerData['hash_id']))
                                imageDownloadList.write('%s%s%s.jpg\n\tdir=%s\n\tout=%s%s.jpg\n' %
                                                        (self.config.get('rjavan', 'baseImageURL'),
                                                         singerData['image_link'],
                                                         self.config.get('rjavan', 'imageResizePostfix'),
                                                         self.config.get('output', 'singersImages'),
                                                         singerData['hash_id'],
                                                         self.config.get('rjavan', 'imageResizePostfix')
                                                         ))
                                singersHashList.append(singerData['hash_id'])
                            except:
                                # Rollback in case there is any error
                                self.conn.rollback()
                        else:
                            print("%s Already Exist !" % (singerData['name'].replace('%', '%%')))
                            singerDuplicateCounter += 1

                        if singerDuplicateCounter >= self.config.getint('rjavan', 'maxDuplicatedSinger'):
                            break

                        f.write(singerInfo[0] + "\r\n")
                else:
                    print("nothing Found")


    def _crawlSingerSongs(self, singerId='', singerUniqueString=''):

        songData = {'link': '',
                    'alt_title': '',
                    'server_id': '',
                    'image_link': '',
                    'artist_name': '',
                    'song_name': '',
                    'hash_id': '',
                    'singer_id': '',
                    'composer': '',
                    'producer': '',
                    'poem': '',
                    'lyric': '',
                    'release_date': '',
                    'photographer': '',
                    'graphist': '',
                    'sponser': '',
                    'likes': '',
                    'dislikes': '',
                    'views': '',
                    'video_link': '',
                    'cover_link': ''
                    }

        songsHashList = []

        imageDownloadList = open(self.config.get('output', 'songsImages') +
                                 self.config.get('output', 'songsImagesDownloadListFile'), "w")

        cursor = self.conn.cursor(dictionary=True)
        if not singerId:
            sql = "SELECT * FROM rj_singers WHERE link=%s"
            # cursor.execute("""SELECT * FROM rj_singers WHERE 'link'=%s""", (singerId))
            cursor.execute(sql, (singerUniqueString,))
        else:
            sql = "SELECT * FROM rj_singers WHERE id=%s"
            cursor.execute(sql, (singerId,))

        singerInfo = cursor.fetchone()

        if cursor.rowcount < 1:
            print("No singer exist with ID=%s or Link=%s" % (singerId, singerUniqueString))
        else:

            # program works in offline mode (good for debug)
            if self.config.getint('mode', 'offline'):
                print()
                # print("we are offline !")  # not implemented yet
            # program is in online mode
            else:
                # print("Online Mode !")
                session = Session()
                # if proxy is enabled
                if self.config.get('network', 'proxy'):
                    proxies = {'https': self.config.get('network', 'proxy')}
                    session.proxies = proxies
                    # HEAD requests ask for *just* the headers, which is all you need to grab the
                    # session cookie

                session.head(self.config.get('rjavan', 'baseURL'))

                # get list of singer's songs
                response = session.get(
                    url='%s?artist=%s&type=mp3' % (self.config.get('rjavan', 'baseSingerSongListURL'),
                                                   re.sub(r"^\W+", "", singerInfo['link'])),
                    # data={
                    #     'artist': re.sub(r"^\W+", "", singerInfo['link']),
                    #     'type': 'mp3'
                    # },
                    headers={
                        'User-Agent': self.config.get('network', 'userAgent'),
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        # 'X-Requested-With': 'XMLHttpRequest',
                        'Referer': self.config.get('rjavan', 'baseURL')
                    }
                )
                sampleText = response.text

                songPattern = r'<li\sclass=\"result\">.*?href=\"\/mp3s\/mp3\/(.*?)\">.*?alt=\"(.*?)\".*?src=\"https:\/\/assets\.rdjvn\-assets\.com\/static\/mp3\/(.*?)\".*?\"artist_name\">(.*?)<.*?\"song_name\">(.*?)<.*?\"views\">(\d{1,3}(,\d{3})*(\.\d+)?).*?mp3id=\"([0-9]+?)\"><\/i>'
                songs = re.findall(songPattern, sampleText, re.S | re.I)

                query = "INSERT INTO rj_songs(english_name, farsi_name, singer_id, url, views, cover_thumb_url, server_id, hash_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)" \
                        " ON DUPLICATE KEY UPDATE english_name=%s,farsi_name=%s,singer_id=%s,url=%s,views=%s,cover_thumb_url=%s,server_id=%s,hash_id=%s"

                if songs:
                    for songInfo in songs:
                        songData['singer_id'] = singerInfo['id']  # singer_id
                        songData['link'] = songInfo[0]  # url
                        songData['alt_title'] = songInfo[1]  # ---
                        songData['image_thumb_link'] = songInfo[2]  # cover_thumb_url
                        songData['artist_name'] = songInfo[3]  # ---
                        songData['song_name'] = songInfo[4]  # english_name, farsi_name
                        songData['views'] = int(songInfo[5].replace(',', ''))  # views
                        songData['server_id'] = int(songInfo[8])  # server_id
                        hashString = songData['link'] + self.config.get('general', 'hashIdBaseString')
                        songData['hash_id'] = hashlib.md5(hashString.encode('utf-8')).hexdigest()  # hash_id

                        if songData['hash_id'] not in songsHashList:
                            args = (songData['song_name'], songData['song_name'], songData['singer_id'],
                                    songData['link'], songData['views'], songData['image_thumb_link'],
                                    songData['server_id'], songData['hash_id'],
                                    songData['song_name'], songData['song_name'], songData['singer_id'],
                                    songData['link'], songData['views'], songData['image_thumb_link'],
                                    songData['server_id'], songData['hash_id'])
                            try:
                                # Execute the SQL command
                                cursor.execute(query, args)
                                # Commit your changes in the database
                                self.conn.commit()
                                print("%s Added !" % (songData['song_name'].replace('%', '%%')))

                                imageDownloadList.write('%s%s\n\tdir=%s\n\tout=%s-thumb.jpg\n' %
                                                        (self.config.get('rjavan', 'baseImageURL'),
                                                         songData['image_thumb_link'],
                                                         self.config.get('output', 'songsImages'),
                                                         songData['hash_id']))
                                imageDownloadList.flush()
                                # print('%s%s\n\tdir=%s\n\tout=%s-thumb.jpg\n' %
                                #       (self.config.get('rjavan', 'baseImageURL'),
                                #        songData['image_thumb_link'],
                                #        self.config.get('output', 'songsImages'),
                                #        songData['hash_id']))
                                songsHashList.append(songData['hash_id'])
                            except Exception as e:
                                print('DB exception: %s' % e)
                                # Rollback in case there is any error
                                self.conn.rollback()
                        else:
                            print("%s Already Exist !" % (songData['song_name'].replace('%', '%%')))
                            # singerDuplicateCounter += 1

                            # if singerDuplicateCounter >= self.config.getint('rjavan', 'maxDuplicatedSinger'):
                            #     break
                            #
                            # f.write(singerInfo[0] + "\r\n")
                else:
                    print("nothing Found")

                    # get background image of singer, if exist

        cursor.close()
        imageDownloadList.close()


    def crawlSongs(self):
        cursor = self.conn.cursor(dictionary=True)

        sql = "SELECT COUNT(*) FROM rj_singers"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        for i in range(0, total_rows, self.config.getint('general', 'songsBatchSize')):

            sql = "SELECT * FROM rj_singers LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('general', 'songsBatchSize'), i))

            singersDataList = []
            for row in cursor:
                singersDataList.append(row)

            for row in singersDataList:
                print("--> Crawling %s Songs:" % row['english_name'])
                self._crawlSingerSongs(singerId=row['id'])

        cursor.close()


    def updateSongsData(self):
        cursor = self.conn.cursor(dictionary=True)
        imageDownloadList = open(self.config.get('output', 'songsImages') +
                                 self.config.get('output', 'songsImagesDownloadListFile'), "a")

        resume = ut.ResumeOperation(self.config.get('output', 'songsUpdateResumeFile'), 0)

        sql = "SELECT COUNT(*) FROM rj_songs"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        cnt = resume.getLastNumber()
        for i in range(cnt, total_rows, self.config.getint('general', 'songsBatchSize')):

            sql = "SELECT * FROM rj_songs LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('general', 'songsBatchSize'), i))

            songDataList = []
            for row in cursor:
                songDataList.append(row)

            for row in songDataList:
                songLink = ('%s%s%s')%(self.config.get('rjavan', 'baseURL'),
                                       self.config.get('rjavan', 'baseSongURL'),
                                       row['url'])

                songData = {'link': '',
                            'alt_title': '',
                            'server_id': '',
                            'image_link': '',
                            'artist_name': '',
                            'song_name': '',
                            'hash_id': '',
                            'singer_id': '',
                            'composer': '',
                            'producer': '',
                            'mix': '',
                            'poet': '',
                            'lyric': '',
                            'added_date': '',
                            'photographer': '',
                            'graphist': '',
                            'sponser': '',
                            'likes': '',
                            'dislikes': '',
                            'views': '',
                            'video_link': '',
                            'song_url': '',
                            'cover_link': '',
                            'description': '',
                            'itunes': '',
                            'amazon': ''
                            }

                singerData = {'facebook': '',
                              'instagram': '',
                              'telegram': ''}

                songData['id'] = row['id']
                songData['singer_id'] = row['singer_id']
                songData['hash_id'] = row['hash_id']
                songData['song_name'] = row['english_name']
                songData['song_url'] = row['song_url']


                # program works in offline mode (good for debug)
                if self.config.getint('mode', 'offline'):
                    self.logger.info("we are offline !")  # not implemented yet
                # program is in online mode
                else:
                    # print("Online Mode !")
                    session = Session()
                    # if proxy is enabled
                    if self.config.get('network', 'proxy'):
                        proxies = {'https': self.config.get('network', 'proxy')}
                        session.proxies = proxies
                        # HEAD requests ask for *just* the headers, which is all you need to grab the
                        # session cookie

                    session.head(self.config.get('rjavan', 'baseURL'))

                    # get list of singer's songs
                    response = session.get(
                        url=songLink,
                        data={},
                        headers={
                            'User-Agent': self.config.get('network', 'userAgent'),
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            # 'X-Requested-With': 'XMLHttpRequest',
                            'Referer': self.config.get('rjavan', 'baseURL')
                        }
                    )
                    sampleText = response.text

                    # with open('./samples/error-sample6.html', 'r', encoding='utf-8') as myfile:
                    #     sampleText = myfile.read()

                    invalidSocials = ['2008/fbml', 'tr?id=547646758704475&amp;ev=PixelInitialized',
                                      'radiojavan']
                    invalidMarkets = ['app/apple-store/id286225933?pt=15757&ct=rjweb&mt=8']

                    songDataPattern = r'\"song_info_panel\".*?\"https:\/\/assets\.rdjvn\-assets\.com\/static\/mp3\/(.*?)\"\/>.*?\"artist_name\">(.*?)<.*?\"song_name\">(.*?)<\/'
                    # statPattern = r'>Plays:\s(\d{1,3}(,\d{3})*(\.\d+)?)<.*?added:\s(.*?)<.*?>(\d{1,3}(,\d{3})*(\.\d+)?)\slikes<.*?>(\d{1,3}(,\d{3})*(\.\d+)?)\sdislikes<'
                    # statPattern = r'>Plays:(\s+)(\d{1,3}(,\d{3})*(0\+)*)<.*?added:(\s+)(.*?)<\/.*?\"rating\">(\d{1,3}(,\d{3})*(0\+)*)\s{1,}(like|likes)<.*?\"rating\">(\d{1,3}(,\d{3})*(0\+)*)\s{1,}(dislike|dislikes)<'
                    statPattern = r'>Plays:(\s+)((0\+)|(\d{1,3}(,\d{3})*))<.*?added:(\s+)(.*?)<\/.*?\"rating\">((0\+)|(\d{1,3}(,\d{3})*))\s+(like|likes)<.*?\"rating\">((0\+)|(\d{1,3}(,\d{3})*))\s+(dislike|dislikes)<'
                    songDescPattern = r'class=\"mp3_description\">(.*?)<div'
                    socialNetPattern = r'\"(http|https){1}:\/\/(www\.){0,1}(instagram|facebook){1}\.com\/(.*?)\"'
                    musicMarketPattern = r'(itunes\.apple\.com\/(.*?)\")|(amazon\.com\/(.*?)\")'
                    tagsPattern = r'class=\"secondary\slabel\".*?href=\"\/mp3s\/browse\/tag\/(.*?)">(.*?)<\/a><\/span>'
                    lyricPattern = r'<div\sclass=\"lyricsFarsi.*?>(.*?)<\/div>'
                    filePattern = r'link=\"https:\/\/rjmediamusic\.com\/media\/mp3\/(.*?)\.mp3\"'

                    songFile = re.findall(filePattern, sampleText, re.S | re.I)
                    stats = re.findall(statPattern, sampleText, re.S | re.I)
                    tags = re.findall(tagsPattern, sampleText, re.S | re.I)
                    data = re.findall(songDataPattern, sampleText, re.S | re.I)
                    descs = re.findall(songDescPattern, sampleText, re.S | re.I)
                    lyrics = re.findall(lyricPattern, sampleText, re.S | re.I)
                    markets = re.findall(musicMarketPattern, sampleText, re.S | re.I)
                    socials = re.findall(socialNetPattern, sampleText, re.S | re.I)

                    if songFile:
                        songData['song_url'] = songFile[0]

                    videoPrefix = ''
                    if data:
                        songData['image_link'] = data[0][0]
                        videoPrefix = re.sub(r'\/.*?\.(jpg|png|jpeg|gif)', '', data[0][0])
                        imageDownloadList.write('%s%s\n\tdir=%s\n\tout=%s.jpg\n' %
                                                (self.config.get('rjavan', 'baseImageURL'),
                                                 songData['image_link'],
                                                 self.config.get('output', 'songsImages'),
                                                 songData['hash_id']))
                        imageDownloadList.flush()

                        if self.config.getint('mode', 'saveHTMLSnapshots'):
                            pageFile = open('%s%s__%s.html'%(self.config.get('output', 'songsPages'),
                                            videoPrefix,
                                            datetime.datetime.now().strftime("%Y-%m-%d")), 'w',
                                            encoding='utf-8')
                            pageFile.write(sampleText)
                            pageFile.close()
                    if stats:
                        songData['views'] = int(stats[0][1].replace(',', '').replace('+', ''))
                        songData['likes'] = int(stats[0][7].replace(',', '').replace('+', ''))
                        songData['dislikes'] = int(stats[0][12].replace(',', '').replace('+', ''))
                        songData['added_date'] = datetime.datetime.strptime(stats[0][6], '%b %d, %Y')

                        if descs:
                            songData['description'] = descs[0].replace('\t', '').replace('</div>', '')
                        if lyrics:
                            songData['lyric'] = lyrics[0].replace('\t', '')

                        composer = re.findall(r'Music:\s(.*?)<', songData['description'], re.S | re.I)
                        if composer:
                            songData['composer'] = composer[0].replace('\t', '').replace('\n', '').strip()

                        poet = re.findall(r'Lyrics:\s(.*?)<', songData['description'], re.S | re.I)
                        if poet:
                            songData['poet'] = poet[0].replace('\t', '').replace('\n', '').strip()

                        producer = re.findall(r'Arrangement:\s(.*?)<', songData['description'], re.S | re.I)
                        if producer:
                            songData['producer'] = producer[0].replace('\t', '').replace('\n', '').strip()

                        mixmaster = re.findall(r'Mix.*?Mastering:\s(.*?)<', songData['description'], re.S | re.I)
                        if mixmaster:
                            songData['mix'] = mixmaster[0].replace('\t', '').replace('\n', '').strip()

                        graphist = re.findall(r'Cover.*?:\s(.*?)<', songData['description'], re.S | re.I)
                        if graphist:
                            songData['graphist'] = graphist[0].replace('\t', '').replace('\n', '').strip()

                        videoPattern = r'<a\shref=\"\/videos\/video\/(%s.*?)\">.*?Watch\sMusic\sVideo<\/i>' %\
                                       (videoPrefix.replace('(', '\(').replace(')', '\)').replace('-', '\-'))
                        video = re.findall(videoPattern, sampleText, re.S | re.I)
                        if video:
                            songData['video_link'] = video[0]

                        if markets:
                            for market in markets:
                                if market[1] not in invalidMarkets:
                                    if 'itunes' in market[0]:
                                        songData['itunes'] = market[1]
                                    if 'amazon' in market[0]:
                                        songData['amazon'] = market[1]

                        if socials:
                            for social in socials:
                                if social[3] not in invalidSocials:
                                    if 'facebook' in market[2]:
                                        singerData['facebook'] = market[3]
                                    if 'instagram' in market[2]:
                                        singerData['instagram'] = market[3]

                        tag = {'song_id': '', 'value': '', 'link': ''}

                        if tags:
                            for tagInfo in tags:
                                tag['song_id'] = row['id']  # song_id
                                tag['value'] = tagInfo[1]  # url
                                tag['link'] = tagInfo[0]  # ---

                                tag['id'] = self._tagExist(cursor, tag['link'])

                                if tag['id']:  # tag already exist

                                    query = "INSERT INTO rj_songs_tags(`song_id`, `tag_id`) VALUES(%s,%s)" \
                                            " ON DUPLICATE KEY UPDATE `song_id`=%s, `tag_id`=%s"
                                    try:
                                        # Execute the SQL command
                                        cursor.execute(query, (tag['song_id'], tag['id'], tag['song_id'], tag['id']))
                                        # Commit your changes in the database
                                        self.conn.commit()

                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()
                                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                        self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                                        self.logger.error('DB exception: %s' % e)
                                        # Rollback in case there is any error
                                        self.conn.rollback()
                                        if self.config.getint('mode', 'stopInException'):
                                            sys.exit(0)

                                else:  # tag not exist
                                    query = "INSERT INTO rj_tags(`value`, `link`) VALUES(%s,%s)" \
                                            " ON DUPLICATE KEY UPDATE `value`=%s, `link`=%s"
                                    try:
                                        # Execute the SQL command
                                        cursor.execute(query, (tag['value'], tag['link'], tag['value'], tag['link']))
                                        # Commit your changes in the database
                                        self.conn.commit()
                                        tag['id'] = cursor.lastrowid

                                        query = "INSERT INTO rj_songs_tags(`song_id`, `tag_id`) VALUES(%s,%s)" \
                                                " ON DUPLICATE KEY UPDATE `song_id`=%s, `tag_id`=%s"
                                        try:
                                            # Execute the SQL command
                                            cursor.execute(query,
                                                           (tag['song_id'], tag['id'], tag['song_id'], tag['id']))
                                            # Commit your changes in the database
                                            self.conn.commit()

                                        except Exception as e:
                                            exc_type, exc_obj, exc_tb = sys.exc_info()
                                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                                            self.logger.error('DB exception: %s' % e)
                                            # Rollback in case there is any error
                                            self.conn.rollback()
                                            if self.config.getint('mode', 'stopInException'):
                                                sys.exit(0)

                                    except Exception as e:
                                        exc_type, exc_obj, exc_tb = sys.exc_info()
                                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                        self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                                        self.logger.error('DB exception: %s' % e)
                                        # Rollback in case there is any error
                                        self.conn.rollback()
                                        if self.config.getint('mode', 'stopInException'):
                                            sys.exit(0)

                        query = "UPDATE rj_songs SET `cover_url`=%s, `views`=%s, `likes`=%s, `dislikes`=%s," \
                                " `description`=%s, `video_url`=%s, `release_date`=%s, `lyric`=%s," \
                                " `english_composer`=%s, `farsi_composer`=%s," \
                                " `english_producer`=%s, `farsi_producer`=%s," \
                                " `english_mixmaster`=%s, `farsi_mixmaster`=%s," \
                                " `english_poet`=%s, `farsi_poet`=%s," \
                                " `english_graphist`=%s, `farsi_graphist`=%s," \
                                " `video_url`=%s, `song_url`=%s," \
                                " `itunes`=%s," \
                                " `amazon`=%s" \
                                " WHERE `id`=%s"
                        try:
                            # print("song id: %d" % songData['id'])
                            cursor.execute(query, (songData['image_link'], songData['views'],
                                                   songData['likes'], songData['dislikes'],
                                                   songData['description'], songData['video_link'],
                                                   songData['added_date'].strftime('%Y-%m-%d'),
                                                   songData['lyric'],
                                                   songData['composer'], songData['composer'],
                                                   songData['producer'], songData['producer'],
                                                   songData['mix'], songData['mix'],
                                                   songData['poet'], songData['poet'],
                                                   songData['graphist'], songData['graphist'],
                                                   songData['video_link'], songData['song_url'],
                                                   songData['itunes'], songData['amazon'],
                                                   songData['id']
                                                   ))
                            self.conn.commit()

                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                            self.logger.error('DB exception: %s' % e)
                            self.conn.rollback()
                            if self.config.getint('mode', 'stopInException'):
                                sys.exit(0)

                        query = "UPDATE rj_singers SET `facebook`=%s, `instagram`=%s, `telegram`=%s WHERE `id`=%s"
                        try:
                            cursor.execute(query, (singerData['facebook'], singerData['instagram'],
                                                   singerData['telegram'], songData['singer_id']
                                                   ))
                            self.conn.commit()

                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                            self.logger.error('DB exception: %s' % e)
                            self.conn.rollback()
                            if self.config.getint('mode', 'stopInException'):
                                sys.exit(0)

                        cnt += 1
                        resume.setLastNumber(cnt)
                        self.logger.info('%d: "id=%d\t%s" Updated Successfully' % (cnt, songData['id'], videoPrefix))


                    else:  # song does not exist anymore !
                        cnt += 1
                        resume.setLastNumber(cnt)
                        self.logger.warning('%d: "id=%d\t%s" Does not Exist Anymore !' % (cnt, songData['id'], videoPrefix))

        imageDownloadList.close()
        cursor.close()





    def updateDownloadLinks(self):
        self.logger.info("updateDownloadLinks method called !")
        cursor = self.conn.cursor(dictionary=True)

        resume = ut.ResumeOperation(self.config.get('rjavan', 'songsLinksUpdateResumeFile'), 0)

        # log handlers
        logger = ut.defineLogger(self.config.get('rjavan', 'songsLinksUpdateLog'), 'inf', True)


        sql = "SELECT COUNT(*) FROM rj_songs"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        cnt = resume.getLastNumber()
        for i in range(cnt, total_rows, self.config.getint('general', 'songsBatchSize')):

            sql = "SELECT * FROM rj_songs LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('general', 'songsBatchSize'), i))

            songDataList = []
            for row in cursor:
                songDataList.append(row)

            for row in songDataList:
                songLink = ('%s%s%s') % (self.config.get('rjavan', 'baseURL'),
                                         self.config.get('rjavan', 'baseSongURL'),
                                         row['url'])

                songData = {'link': '',
                            'alt_title': '',
                            'server_id': '',
                            'image_link': '',
                            'artist_name': '',
                            'song_name': '',
                            'hash_id': '',
                            'singer_id': '',
                            'composer': '',
                            'producer': '',
                            'mix': '',
                            'poet': '',
                            'lyric': '',
                            'added_date': '',
                            'photographer': '',
                            'graphist': '',
                            'sponser': '',
                            'likes': '',
                            'dislikes': '',
                            'views': '',
                            'video_link': '',
                            'song_url': '',
                            'url': '',
                            'cover_link': '',
                            'description': '',
                            'itunes': '',
                            'amazon': ''
                            }

                songData['id'] = row['id']
                songData['singer_id'] = row['singer_id']
                songData['hash_id'] = row['hash_id']
                songData['song_name'] = row['english_name']
                songData['song_url'] = row['song_url']
                songData['url'] = row['url']

                # program works in offline mode (good for debug)
                if self.config.getint('mode', 'offline'):
                    self.logger.info("we are offline !")  # not implemented yet
                # program is in online mode
                else:
                    # print("Online Mode !")
                    session = Session()
                    # if proxy is enabled
                    if self.config.get('network', 'proxy'):
                        proxies = {'https': self.config.get('network', 'proxy')}
                        session.proxies = proxies

                    session.head(self.config.get('rjavan', 'baseURL'))

                    # get list of singer's songs
                    response = session.get(
                        url=songLink,
                        data={},
                        headers={
                            'User-Agent': self.config.get('network', 'userAgent'),
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            # 'X-Requested-With': 'XMLHttpRequest',
                            'Referer': self.config.get('rjavan', 'baseURL')
                        }
                    )
                    sampleText = response.text

                    # with open('./samples/error-sample6.html', 'r', encoding='utf-8') as myfile:
                    #     sampleText = myfile.read()


                    filePattern = r'link=\"(https:\/\/.*?rjmediamusic\.com\/media\/mp3\/.*?\.mp3)\"'

                    songFile = re.findall(filePattern, sampleText, re.S | re.I)

                    if songFile:
                        songData['song_url'] = songFile[0]

                        query = "UPDATE rj_songs SET `song_url`=%s WHERE `id`=%s"
                        try:
                            # print("song id: %d" % songData['id'])
                            cursor.execute(query, (songData['song_url'], songData['id']))
                            self.conn.commit()

                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                            self.logger.error('DB exception: %s' % e)
                            self.conn.rollback()
                            if self.config.getint('mode', 'stopInException'):
                                sys.exit(0)

                        cnt += 1
                        resume.setLastNumber(cnt)
                        logger.info('%d: "id=%d\t%s" Updated Successfully' % (cnt, songData['id'], songData['url']))


                    else:  # song does not exist anymore !
                        cnt += 1
                        resume.setLastNumber(cnt)
                        logger.warning(
                            '%d: "id=%d\t%s" Does not Exist Anymore !' % (cnt, songData['id'], songData['url']))

        cursor.close()

    def _tagExist(self, cursor, tag):
        # sql = "SELECT (1) FROM rj_tags WHERE `link`=%s LIMIT 1"
        # if cursor.execute(sql, (tag,)):
        #     return True
        # else:
        #     return False

        sql = "SELECT id FROM rj_tags WHERE `link`=%s LIMIT 1"
        try:
            cursor.execute(sql, (tag,))
            res = cursor.fetchone()
            if res:
                return res['id']
            else:
                return 0

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
            self.logger.error('DB exception: %s' % e)
            # Rollback in case there is any error
            self.conn.rollback()


    def resetFileResults(self):
        folder = self.config.get('output', 'songsPages')
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                    # elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                self.logger.error(e)
                if self.config.getint('mode', 'stopInException'):
                    sys.exit(0)


    def generateImagesDownloadList(self):
        self.logger.info('generateImagesDownloadList Method Called !')
        cursor = self.conn.cursor(dictionary=True)

        # method level log handler
        logger = ut.defineLogger(self.config.get('rjavan', 'songsGenerateImageDownloadListLog'), 'downlist', True)

        resume = ut.ResumeOperation(self.config.get('rjavan', 'createImageDownloadListResumeFile'), 0)

        if self.config.getint('rjavan', 'songsImagesDownloadListLimit') == 0:
            songImageDownloadList = open(self.config.get('rjavan', 'songsImageDownloadList'), 'a')
            songImageThumbDownloadList = open(self.config.get('rjavan', 'songsThumbImageDownloadList'), 'a')


        elif self.config.getint('rjavan', 'songsImagesDownloadListLimit') > 0:

            fileNumber = int(resume.getLastNumber() / self.config.getint('rjavan', 'songsImagesDownloadListLimit'))
            # directoryName = os.path.dirname(self.config.get('rjavan', 'songsDownloadList128'))
            (imageFileName, imageFileExtension) = os.path.splitext(self.config.get('rjavan', 'songsImageDownloadList'))
            (imageThumbFileName, imageThumbFileExtension) = os.path.splitext(self.config.get('rjavan', 'songsThumbImageDownloadList'))

            # print('%s_%d%s'%(imageFileName, fileNumber, imageFileExtension))
            songImageDownloadList = open('%s_%d%s' % (imageFileName, fileNumber, imageFileExtension), 'a')
            songImageThumbDownloadList = open('%s_%d%s' % (imageThumbFileName, fileNumber, imageThumbFileExtension), 'a')

        else:
            self.logger.info('generateImagesDownloadList Failed ! No Valid songsImagesDownloadListLimit !')
            if self.config.getint('mode', 'stopInException'):
                sys.exit(0)

        sql = "SELECT COUNT(*) FROM rj_songs"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        cnt = resume.getLastNumber() + 1
        validExtensions = ['.jpeg', '.jpg', '.gif', '.png']
        for i in range(cnt, total_rows, self.config.getint('rjavan', 'songsImagesDownloadListLimit')):
            sql = "SELECT * FROM rj_songs LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('rjavan', 'songsImagesDownloadListLimit'), i))

            songDataList = []
            for row in cursor:
                songDataList.append(row)

            for row in songDataList:
                if row['cover_url'] and row['cover_url'] != '':
                    fileExtension = ut.get_ext(row['cover_url'])
                    if fileExtension in validExtensions:
                        songImageDownloadList.write('%s%s\n\tdir=%s\n\tout=%s%s\n' %
                                                  (self.config.get('rjavan', 'baseImageURL'),
                                                      row['cover_url'],
                                                   self.config.get('rjavan', 'songsImages'),
                                                   row['hash_id'], fileExtension))
                        songImageDownloadList.flush()
                        logger.info(
                            '%d\t\"id=%d %s\" image download link Added Successfully !' % (cnt, row['id'], row['url']))
                    else:
                        logger.warning(
                            '%d\t\"id=%d %s\" does not have valid image download link !' % (cnt, row['id'], row['url']))

                else:
                    logger.warning(
                        '%d\t\"id=%d %s\" does not have valid image download link !' % (cnt, row['id'], row['url']))


                if row['cover_thumb_url'] and row['cover_thumb_url'] != '':
                    fileExtension = ut.get_ext(row['cover_thumb_url'])
                    if fileExtension in validExtensions:
                        songImageThumbDownloadList.write('%s%s\n\tdir=%s\n\tout=%s_thumb%s\n' %
                                                  (self.config.get('rjavan', 'baseImageURL'),
                                                      row['cover_thumb_url'],
                                                   self.config.get('rjavan', 'songsImages'),
                                                   row['hash_id'], fileExtension))
                        songImageThumbDownloadList.flush()
                        logger.info(
                            '%d\t\"id=%d %s\" thumb image download link Added Successfully !' % (cnt, row['id'], row['url']))
                    else:
                        logger.warning(
                            '%d\t\"id=%d %s\" does not have valid thumb image download link !' % (cnt, row['id'], row['url']))

                else:
                    logger.warning(
                        '%d\t\"id=%d %s\" does not have valid thumb image download link !' % (cnt, row['id'], row['url']))


                resume.setLastNumber(cnt)

                if cnt % self.config.getint('rjavan', 'songsImagesDownloadListLimit') == 0:
                    songImageDownloadList.close()
                    songImageThumbDownloadList.close()
                    fileNumber = int(cnt / self.config.getint('rjavan', 'songsImagesDownloadListLimit'))

                    songImageDownloadList = open('%s_%d%s' % (imageFileName, fileNumber, imageFileExtension), 'a')
                    songImageThumbDownloadList = open('%s_%d%s' % (imageThumbFileName, fileNumber, imageThumbFileExtension), 'a')

                cnt += 1

        songImageDownloadList.close()
        songImageThumbDownloadList.close()

