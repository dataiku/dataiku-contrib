# This file is the actual code for the Python runnable github_import
from dataiku.runnables import Runnable
from dataikuapi.dss.wiki import DSSWiki
from dataiku.runnables import Runnable, ResultTable
from os import walk
import dataiku
import re
import os
import shutil
import subprocess
import tempfile
import codecs

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return (100, None)

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        rt = ResultTable()
        rt.add_column("article", "Article", "STRING")

        client = dataiku.api_client()
        wiki = DSSWiki(client, self.project_key)
        
        print 'self.config'
        print self.config

        repository_url = 'https://github.com/dataiku/dip'
        if 'repository_url' in self.config:
            repository_url = self.config['repository_url']
            
        wiki_url = repository_url + '/wiki'
        clone_url = repository_url + '.wiki.git'
        if 'username' in self.config and self.config['username'] != '' and 'token' in self.config and self.config['token'] != '':
            username = self.config['username']
            token = self.config['token']
            wiki_url_parts = repository_url.split('://')
            wiki_url_parts.insert(1, username + ":" + token + '@')
            wiki_url_parts.insert(1, '://')
            clone_url = ''.join(wiki_url_parts) + '.wiki.git'
                    
        directory = tempfile.mkdtemp()
        
        if os.path.isdir(directory):
            shutil.rmtree(directory)
        cloneResult = subprocess.call(["git", "clone", clone_url, directory])
        if cloneResult != 0:
            raise Exception('Unable to clone repository. Check your username and token or password.')
                
        files = []
        urls = []
        for (dirpath, dirnames, filenames) in walk(directory):
            for filename in filenames:
                if filename.endswith('.md'):
                    files.append(filename[:-3])
            break

        articles = wiki.list_articles()
        article_ids = []
        for article in articles:
            article_ids.append(article.article_id)

        for filename in files:
            file = open(directory + '/' + filename + '.md', "r")
            
        total_files = len(files)
        processed_files = 0
        for filename in files:
            file = codecs.open(directory + '/' + filename + '.md', encoding='utf-8')
            print 'Importing ' + filename
            rt.add_record([filename])
            processed_files = processed_files + 1
            progress = processed_files*100 / total_files
            progress_callback(progress)

            content = file.read()
            
            pattern = r'(\[.+\]\(' + wiki_url + r'/.+\))'
            items = re.split(pattern, content)
            processed_items = []
            for item in items:
                match = re.match(r'\[.+\]\(' + wiki_url + '/(.+)\)', item)
                if match:
                    str = '[['+ self.project_key + '.' + match.group(1).replace('-', ' ').replace('.', '_') + ']]'
                    processed_items.append(str)
                else:
                    processed_items.append(item)
            print processed_items
            new_content = ''.join(processed_items)
            
            article_id = filename.replace('-', ' ').replace('.', '_')
            if article_id not in article_ids:
                article = wiki.create_article(article_id)
            else:
                article = wiki.get_article(article_id)
            
            article_data = article.get_data()
            article_data.set_body(new_content)
            article_data.save()

        # cleaning up temp directory
        if os.path.isdir(directory):
            shutil.rmtree(directory)

        return rt
