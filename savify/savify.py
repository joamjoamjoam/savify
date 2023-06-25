"""Main module for Savify."""

__all__ = ['Savify']

import os
import re
import shutil
import time, json
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
from shutil import move, Error as ShutilError
from urllib.error import URLError

import validators
import tldextract
import requests
from yt_dlp import YoutubeDL
from ffmpy import FFmpeg, FFRuntimeError

from .utils import PathHolder, safe_path_string, check_env, check_ffmpeg, check_file, create_dir, clean
from .types import *
from .spotify import Spotify
from .track import Track
from .logger import Logger
from .exceptions import FFmpegNotInstalledError, SpotifyApiCredentialsNotSetError, UrlNotSupportedError, \
    YoutubeDlExtractionError, InternetConnectionError


def _sort_dir(track: Track, group: str) -> str:
    if not group:
        return str()

    group = group.replace('%artist%', safe_path_string(track.artists[0]))
    group = group.replace('%album%', safe_path_string(track.album_name))
    group = group.replace('%playlist%', safe_path_string(track.playlist))

    return str(group)


def _progress(data) -> None:
    if data['status'] == 'downloading':
        pass
    elif data['status'] == 'finished':
        pass
    elif data['status'] == 'error':
        raise YoutubeDlExtractionError


class Savify:
    def __init__(self, api_credentials=None, quality=Quality.BEST, download_format=Format.MP3,
                 group=None, path_holder: PathHolder = None, retry: int = 3,
                 ydl_options: dict = None, skip_cover_art: bool = False, logger: Logger = None,
                 ffmpeg_location: str = 'ffmpeg', skip_album_types: list = None) -> None:

        self.download_format = download_format
        self.ffmpeg_location = ffmpeg_location
        self.skip_cover_art = skip_cover_art
        self.downloaded_cover_art = dict()
        self.quality = quality
        self.queue_size = 0
        self.completed = 0
        self.retry = retry
        self.group = group
        self.skip_album_types = [] if skip_album_types is None else skip_album_types 

        # Config or defaults...
        self.ydl_options = ydl_options or dict()
        self.path_holder = path_holder or PathHolder()
        self.logger = logger or Logger(self.path_holder.data_path)

        if api_credentials is None:
            if not check_env():
                raise SpotifyApiCredentialsNotSetError

            self.spotify = Spotify()
        else:
            self.spotify = Spotify(api_credentials=api_credentials)

        if not check_ffmpeg() and self.ffmpeg_location == 'ffmpeg':
            raise FFmpegNotInstalledError

        clean(self.path_holder.get_temp_dir())
        self.check_for_updates()

    def check_for_updates(self) -> None:
        self.logger.info('Checking for updates...')
        latest_ver = requests.get('https://api.github.com/repos/LaurenceRawlings/savify/releases/latest').json()[
            'tag_name']

        from . import __version__
        current_ver = f'v{__version__}'

        if latest_ver == current_ver or True:
            self.logger.info('Savify is up to date!')
        else:
            self.logger.info('A new version of Savify is available, '
                             'get the latest release here: https://github.com/LaurenceRawlings/savify/releases')

    def _parse_query(self, query, query_type=Type.TRACK, artist_albums: bool = False, skip_album_types: list = [], min_tracks: int = 0) -> list:
        result = list()
        if validators.url(query) or query[:8] == 'spotify:':
            if tldextract.extract(query).domain == Platform.SPOTIFY:
                result = self.spotify.link(query, artist_albums=artist_albums, skip_album_types=skip_album_types, min_tracks=min_tracks)
            else:
                raise UrlNotSupportedError(query)

        else:
            if query_type == Type.TRACK:
                result = self.spotify.search(query, query_type=Type.TRACK)

            elif query_type == Type.ALBUM:
                result = self.spotify.search(query, query_type=Type.ALBUM)

            elif query_type == Type.PLAYLIST:
                result = self.spotify.search(query, query_type=Type.PLAYLIST)

            elif query_type == Type.ARTIST:
                result = self.spotify.search(query, query_type=Type.ARTIST, artist_albums=artist_albums, skip_album_types=skip_album_types, min_tracks=min_tracks)

        return result

    def download(self, query, query_type=Type.TRACK, create_m3u=False, artist_albums: bool = False, skip_album_types: list = [], confidence_interval: float = 0.0, min_tracks: int = 0, remove_incomplete_albums: bool = False) -> None:
        try:
            queue = self._parse_query(query, query_type=query_type, artist_albums=artist_albums, skip_album_types=skip_album_types, min_tracks=min_tracks)
            self.queue_size += len(queue)
        except requests.exceptions.ConnectionError or URLError:
            raise InternetConnectionError

        if not (len(queue) > 0):
            self.logger.info('Nothing found using the given query.')
            return

        # set Confidence Interval for Tracks
        for track in queue: track.confidence_interval = confidence_interval * 100

        self.logger.info(f'Downloading {len(queue)} songs...')
        start_time = time.time()
        self.logger.info(f'Using {cpu_count()} cores')
        with ThreadPool(cpu_count()) as pool:
            jobs = pool.map(self._download, queue)

        failed_jobs = list()
        successful_jobs = list()
        for job in jobs:
            if job['returncode'] != 0:
                failed_jobs.append(job)
            else:
                successful_jobs.append(job)

        if create_m3u and len(successful_jobs) > 0:
            track = successful_jobs[0]['track']
            playlist = safe_path_string(track.playlist)

            if not playlist:
                if query_type in {Type.EPISODE, Type.SHOW, Type.ALBUM}:
                    playlist = track.album_name
                elif query_type is Type.ARTIST:
                    playlist = track.artists[0]
                else:
                    playlist = track.name

            m3u = f'#EXTM3U\n#PLAYLIST:{playlist}\n'
            m3u_location = self.path_holder.get_download_dir() / f'{playlist}.m3u'

            for job in successful_jobs:
                track = job['track']
                location = job['location']
                m3u += f'#EXTINF:{str(queue.index(track))},{str(track)}\n'
                from os.path import relpath
                m3u += f'{relpath(location, m3u_location.parent)}\n'

            self.logger.info('Creating the M3U playlist file..')
            with open(m3u_location, 'w') as m3u_file:
                m3u_file.write(m3u)

        self.logger.info('Cleaning up...')
        clean(self.path_holder.get_temp_dir())

        message = f'Download Finished!\n\tCompleted {len(queue) - len(failed_jobs)}/{len(queue)}' \
                  f' songs in {time.time() - start_time:.0f}s\n'

        if len(failed_jobs) > 0:        
            if (query_type == Type.ARTIST or ("artist" in query.lower() and "open.spotify.com" in query.lower())) and artist_albums:
                # Consolidate Albums
                message += f"\n\t{queue[0].artists[0]}'s Album Completion\n"
                albums = {}
                for track in queue:
                    if not track.album_name in albums.keys():
                        albums[track.album_name] = []

                    albums[track.album_name].append(track)
                
                passJobs = [str(x["track"]) for x in successful_jobs]
                passString = ""
                failString = ""
                removingString = ""
                for album in albums:
                    passTracks = []
                    failTracks = []
                    for track in albums[album]:
                        if str(track) in passJobs:
                            passTracks.append(track)
                        else:
                            failTracks.append(track)
                    
                    if len(passTracks) > 0 and len(passTracks) == passTracks[0].album_track_count:
                        passString += f'\n\tAlbum \'{album}\' Tracks {len(passTracks)}/{passTracks[0].album_track_count} (COMPLETE)'
                    else:
                        albumTrack = passTracks[0] if len(passTracks) > 0 else failTracks[0]
                        failString += f'\n\tAlbum \'{album}\' Tracks {len(passTracks)}/{albumTrack.album_track_count} (INCOMPLETE)'
                        if not len(passTracks) == albumTrack.album_track_count: failString += "\n"
                        failTracks = sorted(failTracks, key=lambda x: x.track_number)
                        for failTrack in failTracks:
                            job = [ x for x in failed_jobs if str(x["track"]) == str(failTrack)][0]
                            failString += f'\n\t\tMissing Track #{failTrack.track_number}: {failTrack.name}' \
                                f'\n\t\tReason: {job["error"]}\n'
                        if remove_incomplete_albums:
                            job = [ x for x in failed_jobs if str(x["track"]) == str(failTracks[0])][0]
                            try:
                                shutil.rmtree(os.path.dirname(job["location"]))
                                removingString += f"\tRemoved Incomplete Album: {job['track'].album_name}\n"
                            except:
                                pass
                
                message += passString
                message += failString
                message += removingString

            else:
                message += '\n\tFailed Tracks:\n'
                for failed_job in failed_jobs:
                    message += f'\n\tSong:\t{str(failed_job["track"])}' \
                            f'\n\tReason:\t{failed_job["error"]}\n'



        #self.logger.info(message)
        for line in message.split('\n'):
            self.logger.info(line)


        self.queue_size -= len(queue)
        self.completed -= len(queue)

    def _download(self, track: Track) -> dict:
        extractor = 'ytsearch5'
        if track.platform == Platform.SPOTIFY:
            query = f'{extractor}:{track.artists[0]} - {track.name} podcast' if track.track_type == Type.EPISODE else f'{extractor}:{str(track)} song {"explicit" if track.isExplicit else ""}'
        else:
            query = ''

        output = self.path_holder.get_download_dir() / f'{_sort_dir(track, self.group)}' / safe_path_string(
            f'{str(track)}.{self.download_format}')

        output_temp = f'{str(self.path_holder.get_temp_dir())}/{track.id}.%(ext)s'

        status = {
            'track': track,
            'returncode': -1,
            'location': output,
        }

        if check_file(output):
            status['returncode'] = 0
            self.completed += 1
            self.logger.info(f'Skipped {self.completed} / {self.queue_size} -> {str(track)} {"(Explicit)" if track.isExplicit else ""} is already downloaded. Skipping...')
            return status

        create_dir(output.parent)

        options = {
            'format': 'bestaudio/best',
            'outtmpl': output_temp,
            'restrictfilenames': True,
            'ignoreerrors': True,
            'nooverwrites': True,
            'noplaylist': True,
            'prefer_ffmpeg': True,
            'logger': self.logger,
            'progress_hooks': [_progress],

            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': self.download_format,
                'preferredquality': self.quality,
            }],

            'postprocessor_args': [
                '-write_id3v1', '1',
                '-id3v2_version', '3',
                '-metadata', f'title={track.name}',
                '-metadata', f'album={track.album_name}',
                '-metadata', f'date={track.release_date}',
                '-metadata', f'artist={"/".join(track.artists)}',
                '-metadata', f'disc={track.disc_number}',
                '-metadata', f'track={track.track_number}/{track.album_track_count}',
            ],

            **self.ydl_options,
        }

        output_temp = output_temp.replace('%(ext)s', self.download_format)

        if self.download_format == Format.MP3:
            options['postprocessor_args'].append('-codec:a')
            options['postprocessor_args'].append('libmp3lame')

        if self.ffmpeg_location != 'ffmpeg':
            options['ffmpeg_location'] = self.ffmpeg_location

        attempt = 0
        while attempt < 5:
            attempt += 1

            try:
                with YoutubeDL(options) as ydl:

                    # Search First and Pick a Good URL
                    link = query
                    if track.platform == Platform.SPOTIFY:
                        videos = ydl.extract_info(query, download=False)
                        videos = ydl.sanitize_info(videos)
                        if len(videos['entries']) > 0:
                            videos = videos['entries']
                            self.logger.debug(f"Found {len(videos)} videos for {query}")
                            for video in videos: self.logger.debug(f"\t{video['fulltitle']} ({video['duration_string']}) - {video['id']}")
                            
                            selectedSong = None
                            trackName = re.sub("[\(\[].*?[\)\]]", "", track.name)
                            trackNameSplit = trackName.split(" - ")[0].split(' ')
                            trackNameSplitWithPunc = track.name.split(" - ")[0].split(' ')
                            self.logger.debug(f"Strictly Searching for keywords {trackNameSplit}")
                            kwMatches = 0
                            matchInfo = []

                            for video in videos:
                                kwMatches = 0
                                if len(trackNameSplit) > 0:
                                    for word in trackNameSplit:
                                        if word.upper() in video['fulltitle'].upper():
                                            kwMatches = kwMatches + len(word)
                                confidence = (kwMatches / len("".join(trackNameSplit)))
                                reverseConfidence = len("".join(trackNameSplit)) / len(video['fulltitle'])

                                kwMatchesWPunc = 0
                                if len(trackNameSplitWithPunc) > 0:
                                    for word in trackNameSplitWithPunc:
                                        if word.upper() in video['fulltitle'].upper():
                                            kwMatchesWPunc = kwMatchesWPunc + len(word)
                                confidenceWPunc = (kwMatchesWPunc / len("".join(trackNameSplitWithPunc)))

                                confidence = confidenceWPunc if confidenceWPunc > confidence else confidence

                                if confidence < track.confidence_interval:
                                    newConfidence = 0.0
                                    artistSplit = track.artists[0].split(' ')
                                    if word in artistSplit:
                                        if word.upper() in video['fulltitle'].upper():
                                            kwMatches = kwMatches + len(word)
                                    newConfidence = (kwMatches / (len("".join(trackNameSplit)) + len("".join(artistSplit))))
                                    confidence = newConfidence if newConfidence > confidence else confidence


                                self.logger.debug(f"Video {video['fulltitle']} matches with {round(confidence * 100, 2)}% confidence interval")
                                matchInfo.append((video['fulltitle'], round(confidence*100, 2), round(reverseConfidence*100, 2), video))
                            
                            bestMatch = None
                            if len(matchInfo) > 0:
                                matchInfo = sorted(matchInfo, key=lambda x: x[1], reverse=True)
                                if matchInfo[0][1] >= track.confidence_interval:
                                    if matchInfo[0][1] >= 100:
                                        bestMatch = sorted([x for x in matchInfo if x[1] >= 100], key=lambda x: x[2], reverse=True)[0]
                                    else:    
                                        bestMatch = matchInfo[0]
                                        
                                    selectedSong = bestMatch[3]
                                else:
                                    bestMatch = matchInfo[0]
                                    

                            if selectedSong is not None:
                                link = f"https://www.youtube.com/watch?v={selectedSong['id']}"
                                self.logger.debug(f"Selected Video (CI:{track.confidence_interval}%): {selectedSong['fulltitle']} - {link}")
                            else:
                                self.completed += 1
                                status['returncode'] = 1
                                status['error'] = f"No Matching Video Found using CI of {track.confidence_interval}% Best Match ({bestMatch[1]}): {bestMatch[0]}"
                                self.logger.error(f"Error Downloading {self.completed} / {self.queue_size} -> {str(track)} {'(Explicit)' if track.isExplicit else ''}")
                                self.logger.error(f"No Matching Video Found using CI of {track.confidence_interval}% Best Match ({bestMatch[1]}): {bestMatch[0]}")

                                return status

                    ydl.download([link])
                    if check_file(Path(output_temp)):
                        break

            except YoutubeDlExtractionError as ex:
                if attempt > self.retry:
                    status['returncode'] = 1
                    status['error'] = "Failed to download song."
                    self.logger.error(ex.message)
                    self.completed += 1
                    return status

        if self.download_format != Format.MP3 or self.skip_cover_art:
            try:
                move(output_temp, output)
            except ShutilError:
                status['returncode'] = 1
                status['error'] = 'Filesystem error.'
                self.logger.error('Failed to move temp file!')
                self.completed += 1
                return status

            status['returncode'] = 0
            self.completed += 1
            self.logger.info(f'Downloaded {self.completed} / {self.queue_size} -> {str(track)} {"(Explicit)" if track.isExplicit else ""}')
            return status

        attempt = 0
        while attempt < 5:
            attempt += 1
            cover_art_name = f'{track.album_name} - {track.artists[0]}'

            if cover_art_name in self.downloaded_cover_art:
                cover_art = self.downloaded_cover_art[cover_art_name]
            else:
                try:
                    cover_art = self.path_holder.download_file(track.cover_art_url, extension='jpg')
                    self.downloaded_cover_art[cover_art_name] = cover_art
                except:
                    pass

            ffmpeg = FFmpeg(executable=self.ffmpeg_location,
                            inputs={str(output_temp): None, str(cover_art): None, },
                            outputs={
                                str(
                                    output): '-loglevel quiet -hide_banner -y -map 0:0 -map 1:0 -c copy -id3v2_version 3 '
                                             '-metadata:s:v title="Album cover" -metadata:s:v comment="Cover (front)" '
                                # '-af "silenceremove=start_periods=1:start_duration=1:start_threshold=-60dB:'
                                # 'detection=peak,aformat=dblp,areverse,silenceremove=start_periods=1:'
                                # 'start_duration=1:start_threshold=-60dB:'
                                # 'detection=peak,aformat=dblp,areverse"'
                            }
                            )

            try:
                ffmpeg.run()
                break

            except FFRuntimeError:
                if attempt > self.retry:
                    try:
                        move(output_temp, output)
                        break

                    except ShutilError:
                        status['returncode'] = 1
                        status['error'] = 'Filesystem error.'
                        self.logger.error('Failed to move temp file!')
                        self.completed += 1
                        return status

        status['returncode'] = 0
        try:
            from os import remove
            remove(output_temp)

        except OSError:
            pass

        self.completed += 1
        self.logger.info(f'Downloaded {self.completed} / {self.queue_size} -> {str(track)}')
        return status
