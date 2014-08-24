#!/usr/bin/env python
#
# Funf: Open Sensing Framework
# Copyright (C) 2010-2011 Nadav Aharony, Wei Pan, Alex Pentland.
# Acknowledgments: Alan Gardner
# Contact: nadav@media.mit.edu
# 
# This file is part of Funf.
# 
# Funf is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.
# 
# Funf is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with Funf. If not, see <http://www.gnu.org/licenses/>.
# 

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
import sys
sys.path.insert(0, '/home/shuang/workspace/funfsens/scripts-0.2.3/data_analyze')

import cgi
import urlparse
import os.path
import shutil
import time

import sqlite3
from db_helper import DBHelper
#import audio_features as af
from features import FeatureRecord

server_dir = os.path.dirname(__file__)

config_path = '/config'
config_file_path = os.path.join(server_dir, 'config.json')

upload_path = '/data'
upload_dir = os.path.join(server_dir, 'uploads')

def read_config():
    config = None
    try:
        with open(config_file_path) as config_file:
            config = config_file.read()
    except IOError:
        pass
    return config

def backup_file(filepath):
    shutil.move(filepath, filepath + '.' + str(int(time.time()*1000)) + '.bak')

def write_db(filename, feature_path):
    print 'Writing to db: ' + feature_path

# "/uploads/" + ["train"|"test"] + {feature} + {scenes} + {location}
def write_file(filename, file, feature_path):
    feature_dir = upload_dir + feature_path
    backup_dir = feature_dir.replace("uploads", "backup", 1) + "-" + str(int(time.time()*1000)) + '.bak'
    
    #? Write to temporary db file
    tempdb = os.path.join(feature_dir, 'temp.db')
    if os.path.exists(tempdb):
        os.remove(tempdb)
    with open(tempdb, 'wb') as output_file:
        while True:
            chunk = file.read(1024)
            if not chunk:
                break
            output_file.write(chunk)
    output_file.close()
    #TODO: decrypt single db file
    #? Write to sqllite database
    conn = sqlite3.connect(tempdb)
    cur = conn.cursor()
    cur.execute('SELECT * FROM data')
    r = cur.fetchone()
    frecord = FeatureRecord(r)

    datehour =  frecord.getDateHour()
    conn.close()
    #? Extract data metadate from path
    splits = feature_path.split('/')
    #print splits
    usage = splits[1]; feature = splits[2]; scene = splits[3]
    #print usage, feature, scene, datehour
    #print filename
    dbname = usage + '_' + feature + '_' + scene + '_' + datehour + '.db'
    #TODO: Multiple db files, merge is required
    filepath = os.path.join(feature_dir, dbname) 

    #? Backup if exists
    if os.path.isfile(filepath): # Backup data
        os.makedirs(backup_dir)
        shutil.move(filepath, backup_dir)
        print 'File %s has been backup to %s' % (filepath, backup_dir)
    # Create new data directory
    if not os.path.exists(feature_dir):
        os.makedirs(feature_dir)
    # Rename temp db file
    shutil.move(tempdb, filepath)

class RequestHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        parsed_url = urlparse.urlparse(self.path)
        if parsed_url.path == config_path:
            config = read_config()
            if config:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(config)
            else:
                self.send_error(500)
        elif parsed_url.path == upload_path:
            self.send_error(405)
        else:
            self.send_error(404)
    
    def do_POST(self):
        parsed_url = urlparse.urlparse(self.path)
        path = parsed_url.path
        ctype, pdict = cgi.parse_header(self.headers['Content-Type']) 
        path_comp = path.split('/')     # "/data/" + ["train"|"test"] + {feature} + {scenes} + {location}
        root_path = path_comp[1]
        path_comp.remove(root_path)
        feature_path = '/'.join(path_comp)
        #print feature_path
        if root_path == 'data':
            if ctype=='multipart/form-data':
                form = cgi.FieldStorage(self.rfile, self.headers, environ={'REQUEST_METHOD':'POST'})
                try:
                    fileitem = form["uploadedfile"]
                    if fileitem.file:
                        try:
                            write_file(fileitem.filename, fileitem.file, feature_path)
                        except Exception as e:
                            print e
                            self.send_error(500)
                        else:
                            self.send_response(200)
                            self.end_headers()
                            self.wfile.write("OK")
                        return
                except KeyError:
                    pass
            # Bad request
            self.send_error(400)
        elif parsed_url.path == config_path:
            self.send_error(405)
        else:
            self.send_error(404)
        

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""                

    
if __name__ == '__main__':
    if sys.argv[1:]:
        port = int(sys.argv[1])
    else:
        port = 8000
    server_address = ('', port)
    httpd = ThreadedHTTPServer(server_address, RequestHandler)

    sa = httpd.socket.getsockname()
    print "Serving HTTP on", sa[0], "port", sa[1], "..."
    print 'use <Ctrl-C> to stop'
    httpd.serve_forever()

