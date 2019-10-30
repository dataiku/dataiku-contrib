# -*- coding: utf-8 -*-
import os
import gzip
import zipfile
import requests

from flair.file_utils import *
from flair.embeddings import *
from flair.models.sequence_tagger_model import *

import dataiku
from dataiku.runnables import Runnable


class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()

    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return (100, 'NONE')

    def run(self, progress_callback):

        # Retrieving parameters
        output_folder_name = self.config.get('outputName', '')

        # Creating new Managed Folder if needed
        project = self.client.get_project(self.project_key)
        output_folder_found = False

        for folder in project.list_managed_folders():
            if output_folder_name == folder['name']:
                output_folder = project.get_managed_folder(folder['id'])
                output_folder_found = True
                break

        if not output_folder_found:
            output_folder = project.create_managed_folder(output_folder_name)

        output_folder = dataiku.Folder(output_folder.get_definition()["id"], project_key=self.project_key)
        output_folder_path = output_folder.get_path()

        #######################################
        # Downloading the model
        #######################################

        CACHE_ROOT = output_folder_path

        def get_from_cache(url: str, cache_dir: str = None) -> str:
            """
            Given a URL, look for the corresponding dataset in the local cache.
            If it's not there, download it. Then return the path to the cached file.
            """

            os.makedirs(cache_dir, exist_ok=True)

            filename = re.sub(r'.+/', '', url)
            # get cache path to put the file
            cache_path = os.path.join(cache_dir, filename)
            if os.path.exists(cache_path):
                return cache_path

            # make HEAD request to check ETag
            response = requests.head(url)
            if response.status_code != 200:
                raise IOError("HEAD request failed for url {}".format(url))

            if not os.path.exists(cache_path):

                # GET file object
                req = requests.get(url, stream=True)
                content_length = req.headers.get('Content-Length')
                total = int(
                    content_length) if content_length is not None else None
                progress = Tqdm.tqdm(unit="B", total=total)
                with open(cache_path, 'wb') as temp_file:
                    for chunk in req.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            progress.update(len(chunk))
                            temp_file.write(chunk)

                progress.close()

            return cache_path

        def cached_path(url_or_filename: str, cache_dir: str) -> str:
            """
            Given something that might be a URL (or might be a local path),
            determine which. If it's a URL, download the file and cache it, and
            return the path to the cached file. If it's already a local path,
            make sure the file exists and then return the path.
            """
            dataset_cache = os.path.join(CACHE_ROOT, cache_dir)

            parsed = urlparse(url_or_filename)

            if parsed.scheme in ('http', 'https'):
                # URL, so get it from the cache (downloading if necessary)
                return get_from_cache(url_or_filename, dataset_cache)
            elif parsed.scheme == '' and os.path.exists(url_or_filename):
                # File, and it exists.
                return url_or_filename
            elif parsed.scheme == '':
                # File, but it doesn't exist.
                raise FileNotFoundError(
                    "file {} not found".format(url_or_filename))
            else:
                # Something unknown
                raise ValueError(
                    "unable to parse {} as a URL or as a local path".format(url_or_filename))

        class CustomSequenceTagger(SequenceTagger):
            @staticmethod
            def load(model: str):
                model_file = None
                aws_resource_path = 'https://s3.eu-central-1.amazonaws.com/alan-nlp/resources/models-v0.2'

                if model.lower() == 'ner':
                    base_path = '/'.join([aws_resource_path,
                                          'NER-conll03--h256-l1-b32-%2Bglove%2Bnews-forward%2Bnews-backward--v0.2',
                                          'en-ner-conll03-v0.2.pt'])
                    model_file = cached_path(base_path, cache_dir='models')

                if model.lower() == 'ner-ontonotes':
                    base_path = '/'.join([aws_resource_path,
                                          'NER-ontoner--h256-l1-b32-%2Bcrawl%2Bnews-forward%2Bnews-backward--v0.2',
                                          'en-ner-ontonotes-v0.2.pt'])
                    model_file = cached_path(base_path, cache_dir='models')

                if model_file is not None:
                    tagger: SequenceTagger = SequenceTagger.load(model_file)
                    return tagger

        tagger = CustomSequenceTagger.load('ner-ontonotes')

        return "<br><span>The model was downloaded successfuly !</span>"
