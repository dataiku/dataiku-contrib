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

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        return (100, None)

    def run(self, progress_callback):
        # Prepare the result
        rt = ResultTable()
        rt.add_column("article", "Article", "STRING")
        rt.add_column("status", "Status", "STRING")

        client = dataiku.api_client()
        wiki = DSSWiki(client, self.project_key)
        
        # Clone the Git repository into a local temporary directory
        repository_url = self.config['repository_url']
        if repository_url == "":
            raise Exception("Missing Github repository URL")
        username = self.config['username']
        if username == "":
            raise Exception("Missing Github username")
        token = self.config['token']
        if token == "":
            raise Exception("Missing Github token or password")
        wiki_url_parts = repository_url.split('://')
        wiki_url_parts.insert(1, username + ":" + token + '@')
        wiki_url_parts.insert(1, '://')
        wiki_git_clone_url = ''.join(wiki_url_parts) + '.wiki.git'
 
        directory = tempfile.mkdtemp()
        cloneResult = subprocess.check_call(["git", "clone", wiki_git_clone_url, directory])
                
        # Enumerate all Github articles found at the root of the repository (i.e. all files with a .md extension)
        files = []
        urls = []
        for (dirpath, dirnames, filenames) in walk(directory):
            for filename in filenames:
                if filename.endswith('.md'):
                    files.append(filename[:-3])
            break

        # Enumerate all existing articles on DSS
        articles = wiki.list_articles()
        article_ids = []
        for article in articles:
            article_ids.append(article.article_id)

        # Import all Github articles into DSS
        total_files = len(files)
        processed_files = 0
        for filename in files:
            # Read file
            print 'Importing ' + filename
            file = codecs.open(directory + '/' + filename + '.md', encoding='utf-8')
            content = file.read()
            
            # Update progress and result
            rt.add_record([filename, "Imported"])
            processed_files = processed_files + 1
            progress = processed_files*100 / total_files
            progress_callback(progress)

            # Reencode all hardcoded linked towards Github web site into classic relative links.
            pattern = r'(\[.+\]\(' + repository_url + r'/wiki/.+\))'
            items = re.split(pattern, content)
            processed_items = []
            for item in items:
                match = re.match(r'\[.+\]\(' + repository_url + '/wiki/(.+)\)', item)
                if match:
                    str = '[['+ self.project_key + '.' + match.group(1).replace('-', ' ').replace('.', '_') + ']]'
                    processed_items.append(str)
                else:
                    processed_items.append(item)
            print processed_items
            new_content = ''.join(processed_items)
            
            # Github uses '-' to encode spaces, and DSS does not accept dots ('.') into article IDs
            article_id = filename.replace('-', ' ').replace('.', '_')
            if article_id not in article_ids:
                article = wiki.create_article(article_id)
            else:
                article = wiki.get_article(article_id)
            
            article_data = article.get_data()
            article_data.set_body(new_content)
            article_data.save()

        # Cleaning up temp directory
        if os.path.isdir(directory):
            shutil.rmtree(directory)

        return rt
