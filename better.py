#!/usr/bin/python3
import os
import subprocess
import shlex
import shutil
import sys
import re
import multiprocessing
import time
import argparse

# Your unique announce URL
announce = ''

# Where to output .torrent files
torrent_output = '.'

# Where to save transcoded albums
transcode_output = '.'

# The default formats to transcode to
default_formats = '320,v0'

# Whether or not to transcode by default
default_transcode = True

# Whether or not to make .torrent files by default
default_torrent = True

# The maximum number of threads to maintain. Any number less than 1 means the
# script will use the number of CPU cores in the system. This is the default
# value for the -c (--cores) option.
max_threads = 0

# I prefix torrents I download as FL for Freeleech, UL for Upload, etc. Any
# prefix in this set will be removed from any transcoded albums and from the
# resulting torrent files created.
ignored_prefixes = {
}

# torrent_commands is the set of all ways to create a torrent using various
# torrent clients. These are the following replacements:
# {0}: Source directory to create a torrent from
# {1}: Output .torrent file
# {2}: Your announce URL
torrent_commands = {
    'transmission-create -p -o {1} -t {2} {0}',
    'mktorrent -p -o {1} -a {2} {0}'
}
torrent_command = None

# transcode_commands is the map of how to transcode into each format. The
# replacements are as follows:
# {0}: The input file (*.flac)
# {1}: The output file (*.mp3 or *.m4a)
ffmpeg = 'ffmpeg -threads 1 '
transcode_commands = {
    'alac': ffmpeg + '-i {0} -acodec alac {1}',
    '320': ffmpeg + '-i {0} -acodec libmp3lame -ab 320k {1}',
    'v0': ffmpeg + '-i {0} -qscale:a 0 {1}',
    'v1': ffmpeg + '-i {0} -qscale:a 1 {1}',
    'v2': ffmpeg + '-i {0} -qscale:a 2 {1}'
}

# extensions maps each codec type to the extension it should use
extensions = {
    'alac': 'm4a',
    '320': 'mp3',
    'v0': 'mp3',
    'v2': 'mp3'
}

# codecs is use in string matching. If, in naming an album's folder name, you
# would use [FLAC] or [ALAC] or [320], then the lowercase contents of the
# brackets belongs in codecs so it can be matched and replaced with the
# transcode codec type.
codecs = {
    'flac', 'flac 24bit', 'flac 16-44', 'flac 16-48', 'flac 24-44', 'flac 24-48', 'flac 24-96', 'flac 24-196',
    '16-44', '16-48', '24-44', '24-48', '24-96', '24-196',
    'alac',
    '320', '256', '224', '192',
    'v0', 'apx', '256 vbr', 'v1', '224 vbr', 'v2', 'aps', '192 vbr'
}

# The list of lossless file extensions. While m4a can be lossy, it's up to you,
# the user, to ensure you're only transcoding from a lossless source material.
LOSSLESS_EXT = {'flac', 'wav', 'm4a'}

# The list of lossy file extensions
LOSSY_EXT = {'mp3', 'aac', 'opus', 'ogg', 'vorbis'}

# The version number
__version__ = '0.4 dev'

exit_code = 0
FILE_NOT_FOUND = 1 << 0
ARG_NOT_DIRECTORY = 1 << 1
NO_TORRENT_CLIENT = 1 << 2
TRANSCODE_AGAINST_RULES = 1 << 3
TRANSCODE_DIR_EXISTS = 1 << 4
UNKNOWN_TRANSCODE = 1 << 5
NO_ANNOUNCE_URL = 1 << 6
NO_TRANSCODER = 1 << 7
TORRENT_ERROR = 1 << 8
TRANSCODE_ERROR = 1 << 9


def enumerate_contents(directory):
    has_lossy = False
    lossless_files = []
    data_files = []
    directories = []

    for root, _, files in os.walk(directory):
        root = root[len(directory):].lstrip('/')

        if len(root) > 0:
            directories.append(root)

        for file in files:
            extension = file[file.rfind('.') + 1:]
            if len(root) > 0:
                file = root + '/' + file

            if extension in LOSSLESS_EXT:
                lossless_files.append(file)
            else:
                if extension in LOSSY_EXT:
                    has_lossy = True
                data_files.append(file)

    return directories, data_files, has_lossy, lossless_files


def format_command(command, *args):
    safe_args = [shlex.quote(arg) for arg in args]
    return command.format(*safe_args)


def command_exists(command):
    return shutil.which(shlex.split(command)[0]) is not None


def find_torrent_command(commands):
    for command in commands:
        if command_exists(command):
            return command

    return None


def copy_contents(src, dst, dirs, files):
    os.mkdir(dst)

    for subdir in dirs:
        os.mkdir(dst + '/' + subdir)

    for file in files:
        shutil.copy(src + '/' + file, dst + '/' + file)


def make_torrent(directory, output, announce_url):
    global torrent_command, exit_code
    print('Making torrent for ' + directory)

    if torrent_command is None:
        torrent_command = find_torrent_command(torrent_commands)
        if torrent_command is None:
            print('No torrent client found, can\'t create a torrent')
            exit_code |= NO_TORRENT_CLIENT
            return

    command = format_command(torrent_command, directory, torrent_output + '/' + output, announce_url)
    torrent_status = subprocess.call(command, shell=True)
    if torrent_status != 0:
        print('Making torrent file exited with status {}!'.format(torrent_status))
        exit_code |= TORRENT_ERROR


# noinspection PyUnresolvedReferences
def transcode_files(src, dst, files, command, extension):
    global exit_code
    remaining = files[:]
    transcoded = []
    threads = [None] * max_threads

    transcoding = True

    while transcoding:
        transcoding = False

        for i in range(len(threads)):
            if threads[i] is None or threads[i].poll() is not None:
                if threads[i] is not None:
                    if threads[i].poll() != 0:
                        print('Error transcoding, process exited with code {}'.format(threads[i].poll()))
                        print('stderr output...')
                        print(threads[i].communicate()[1].encode('utf-8', 'surrogateescape').decode('utf-8', 'ignore'))
                    threads[i].kill()

                threads[i] = None

                if len(remaining) > 0:
                    transcoding = True
                    file = remaining.pop()
                    transcoded.append(dst + '/' + file[:file.rfind('.') + 1] + extension)
                    threads[i] = subprocess.Popen(
                        format_command(command, src + '/' + file, transcoded[-1]), stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, shell=True, universal_newlines=True
                    )
                    print(
                        'Transcoding {} ({} remaining)'.format(file, len(remaining)).encode(
                            'utf-8', 'surrogateescape'
                        ).decode('utf-8', 'ignore')
                    )
            else:
                transcoding = True

        time.sleep(0.05)

    for file in transcoded:
        if not os.path.isfile(file):
            print('An error occurred and {} was not created'.format(file))
            exit_code |= TRANSCODE_ERROR
        elif os.path.getsize(file) == 0:
            print('An error occurred and {} is empty'.format(file))
            exit_code |= TRANSCODE_ERROR


def transcode_album(source, directories, files, lossless_files, formats, explicit_transcode, mktorrent):
    global exit_code

    codec_regex = r'\[(' + '|'.join([codec for codec in codecs]) + r')\](?!.*\/.*)'
    dir_has_codec = re.search(codec_regex, source, flags=re.IGNORECASE) is not None

    for transcode_format in formats:
        if not command_exists(transcode_commands[transcode_format]):
            command = shlex.split(transcode_commands[transcode_format])[0]
            print('Cannot transcode to ' + transcode_format + ', "' + command + '" not found')
            exit_code |= NO_TRANSCODER
            continue

        print('\nTranscoding to ' + transcode_format)

        if dir_has_codec:
            transcoded = re.sub(codec_regex, '[{}]'.format(transcode_format.upper()), source, flags=re.IGNORECASE)
        else:
            transcoded = source.rstrip() + ' [{}]'.format(transcode_format.upper())

        transcoded = transcoded[transcoded.rfind('/') + 1:]

        for prefix in ignored_prefixes:
            if transcoded.startswith(prefix):
                transcoded = transcoded[len(prefix):]
                break

        transcoded = transcode_output + '/' + transcoded

        if os.path.exists(transcoded):
            if explicit_transcode:
                exit_code |= TRANSCODE_DIR_EXISTS

            print('Directory already exists: ' + transcoded)
            continue

        copy_contents(source, transcoded, directories, files)
        transcode_files(source, transcoded, lossless_files, transcode_commands[transcode_format],
                        extensions[transcode_format])

        if mktorrent:
            make_torrent(transcoded, transcoded[transcoded.rfind('/'):] + '.torrent', announce)


def is_transcode_allowed(has_lossy, lossless_files, explicit_transcode):
    global exit_code

    if has_lossy > 0:
        if len(lossless_files) == 0:
            print('Cannot transcode lossy formats, exiting')
            exit_code |= TRANSCODE_AGAINST_RULES
            return False
        elif not explicit_transcode:
            print('Found mixed lossy and lossless, you must explicitly enable transcoding')
            exit_code |= TRANSCODE_AGAINST_RULES
            return False

    if len(lossless_files) == 0:
        print('Nothing to transcode!')
        exit_code |= TRANSCODE_AGAINST_RULES
        return False

    return True


def check_main_args(directory, transcode_formats, explicit_torrent):
    global exit_code
    code = 0

    if not os.path.exists(directory):
        print('The directory "{}" doesn\'t exist'.format(directory))
        code |= FILE_NOT_FOUND
    elif os.path.isfile(directory):
        print('The file "{}" is not a directory'.format(directory))
        code |= ARG_NOT_DIRECTORY

    for i in range(len(transcode_formats)):
        transcode_formats[i] = transcode_formats[i].lower()

        if transcode_formats[i] not in transcode_commands.keys():
            print('No way of transcoding to ' + transcode_formats[i])
            code |= UNKNOWN_TRANSCODE

    if explicit_torrent and (announce is None or len(announce) == 0):
        print('You cannot create torrents without first setting your announce URL')
        code |= NO_ANNOUNCE_URL

    exit_code |= code

    return code == 0


def process_album(directory, do_transcode, explicit_transcode, transcode_formats, do_torrent, explicit_torrent,
                  original_torrent):
    global exit_code
    directory = os.path.abspath(directory)

    if not (check_main_args(directory, transcode_formats, explicit_torrent)):
        return

    if original_torrent:
        make_torrent(directory, directory[directory.rfind('/'):] + '.torrent', announce)

    if do_transcode:
        directories, data_files, has_lossy, lossless_files = enumerate_contents(directory)

        if is_transcode_allowed(has_lossy, lossless_files, explicit_transcode):
            transcode_album(directory, directories, data_files, lossless_files, transcode_formats, explicit_transcode,
                            do_torrent)


def parse_args():
    description = '(Version {}) Transcode albums and create torrents in one command. Default behavior can be changed ' \
                  'by opening %(prog)s with a text editor and changing the variables at the top of the file.' \
        .format(__version__)

    parser = argparse.ArgumentParser(description=description)
    transcode_group = parser.add_mutually_exclusive_group()
    torrent_group = parser.add_mutually_exclusive_group()

    parser.add_argument('album', help='The album to process', nargs='+')

    parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)

    announce_postfix = ' (Usable URL set)' if len(announce) > 0 else ''
    parser.add_argument('-a', '--announce', action='store', default=announce,
                        help='The torrent announce URL to use' + announce_postfix)

    postfixes = {
        't': ' (default)' if default_transcode else '',
        'T': ' (default)' if not default_transcode else '',
        'm': ' (default)' if default_torrent else '',
        'M': ' (default)' if not default_torrent else ''
    }
    transcode_group.add_argument('-t', '--transcode', action='store_true',
                                 help='Transcode the given album into other formats' + postfixes['t'])
    transcode_group.add_argument('-T', '--no-transcode', action='store_true',
                                 help='Ensures the given album is NOT transcoded' + postfixes['T'])

    torrent_group.add_argument('-m', '--make-torrent', action='count', default=0,
                               help='Creates a torrent of any transcoded albums. Specify more than once to also create '
                                    'a torrent of the source album (e.g. -mm).' + postfixes['m'])
    torrent_group.add_argument('-M', '--no-torrent', action='store_true',
                               help='Ensures no .torrent files are created' + postfixes['M'])

    parser.add_argument('-f', '--formats', action='store', default=default_formats,
                        help='The comma-separated formats to transcode to (can be of alac,320,v0,v1,v2) '
                             '(default: %(default)s)')
    parser.add_argument('-c', '--cores', action='store', type=int, default=max_threads,
                        help='The number of cores to transcode on. Any number below 1 means to use the '
                             'number of CPU cores in the system (default: %(default)s)')

    parser.add_argument('-o', '--torrent-output', action='store', default=torrent_output,
                        help='The directory to store any created .torrent files (default: %(default)s)')
    parser.add_argument('-O', '--transcode-output', action='store', default=transcode_output,
                        help='The directory to store any transcoded albums in (default: %(default)s)')

    return parser.parse_args()


def main(args):
    global exit_code, announce, torrent_output, transcode_output, max_threads

    announce = args.announce

    do_transcode = default_transcode and not args.no_transcode
    explicit_transcode = args.transcode
    formats = args.formats.split(',')

    do_torrent = default_torrent and not args.no_torrent
    explicit_torrent = args.make_torrent
    original_torrent = args.make_torrent == 2

    if not explicit_torrent and len(announce) == 0:
        do_torrent = False

    torrent_output = args.torrent_output
    transcode_output = args.transcode_output

    max_threads = args.cores
    if max_threads < 1:
        max_threads = multiprocessing.cpu_count()

    if not os.path.isdir(torrent_output):
        print('The given torrent output dir ({}) is not a directory'.format(torrent_output))
        exit_code |= ARG_NOT_DIRECTORY
    elif not os.path.isdir(transcode_output):
        print('The given transcode output dir ({}) is not a directory'.format(transcode_output))
        exit_code |= ARG_NOT_DIRECTORY

    if exit_code != 0:
        return

    first_print = True

    for album in args.album:
        if not first_print:
            print('\n\n')
        first_print = False

        print('Processing ' + album)
        process_album(album, do_transcode, explicit_transcode, formats, do_torrent, explicit_torrent, original_torrent)


main(parse_args())
if exit_code != 0:
    print('An error occurred, exiting with code {}'.format(exit_code))
sys.exit(exit_code)
