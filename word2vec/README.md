# Installation

You generally don't need any manual installation. The plugin can install the required packages automatically

## Manual installation

You will need to <a href="https://radimrehurek.com/gensim/install.html">install gensim</a> on DSS virtual environment. To do so, follow the <a href="http://doc.dataiku.com/dss/latest/installation/python.html">classic python installation procedure</a> with pip.

To speed up the process, make sure you have a C compiler before installing gensim (70x speedup compared to plain implementation).

# Usage

This plugin uses the word2vec implementation available in gensim. For more information about the parameters, see documentation <a href="https://radimrehurek.com/gensim/models/word2vec.html">here</a>. 

The "Train word2vec" recipe takes a dataset with a text column as input and train a word2vec model on it. The output is a word2vec model in a managed folder.

The "Apply word2vec" recipes applies a learnt word2vec transformation on a column of words or sentence. It takes a folder with a trained word2vec model and a text dataset as input.

Note that it is possible to use a model trained outside DSS:

 *  by changing the "model name" parameter (for example <a href="https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit?usp=sharing">this one</a>, see <a href="https://code.google.com/p/word2vec/">here</a> for more).