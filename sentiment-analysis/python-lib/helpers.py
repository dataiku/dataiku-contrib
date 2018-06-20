# -*- coding: utf-8 -*-

import numpy as np
from collections import Counter
    
from fastText import load_model


def detect_language_using_fasttext(fasttext_model_path, texts):
    """
    Identifies the language of a sample of texts and returns the most common language.
    
    """
    
    language_detection_model = load_model(fasttext_model_path)

    random_texts = np.random.choice(texts, size=min(len(texts), 100), replace=False)
    
    predicted_languages, _ = language_detection_model.predict([s.replace('\n', ' ') for s in random_texts])
    
    text_language = Counter([v[0] for v in predicted_languages]).most_common()
    text_language = text_language[0][0].split('__')[-1]

    if text_language == "en":
        return "english"
    elif text_language == "fr":
        return "french"
    else:
        raise NotImplementedError("There are no models yet for language : {}".format(text_language))