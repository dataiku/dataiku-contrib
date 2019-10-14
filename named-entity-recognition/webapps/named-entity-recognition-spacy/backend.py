from flask import request
import spacy
from spacy import displacy

LANGUAGE = 'en'

try:
    nlp = spacy.load(LANGUAGE)
except IOError:
    import sys
    from subprocess import Popen, PIPE

    # sys.executable returns the complete path to the python executable of the current process
    command = [sys.executable, "-m", "spacy", "download", LANGUAGE]

    p = Popen(command, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()

    try:
        nlp = spacy.load(LANGUAGE)
    except:
        raise Exception("Could not download SpaCy's model, probably because you don't have admin rights over the plugin code environment.")

@app.route('/run_NER')
def run_NER():
    text = request.args.get('input', '')

    doc = nlp(text)

    html = displacy.render(doc, style='ent', page=False)
    return json.dumps(html)
