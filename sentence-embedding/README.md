# Sentence Embedding

This plugin provides a tool for computing numerical sentence representations (also known as **Sentence Embeddings**).

These embeddings can be used as **features** to train a downstream machine learning model (for sentiment analysis for example). But they can also be used to compare texts and compute their similarity using your favorite distance or similarty (like **cosine similarity**).

The plugin comes with several features:
- A macro that allows you to download pre-trained word embeddings from various models: **Word2vec**, **GloVe**, **FastText** or **ELMo**.

- A recipe that allows you to use these vectors to compute sentence embeddings. This recipe relies on one of two possible aggregation methods: 
	- **Simple Average**: which simply averages the word embeddings of a text to get its sentence embedding.
	- **SIF embedding**: which consists in a weighted average of word embeddings, followed by the removal of their first principal component. For more information about SIF, you can refer to [this paper](https://openreview.net/pdf?id=SyK00v5xx) or [this blogpost](http://www.offconvex.org/2018/06/17/textembeddings/).

- A second recipe that allows you to compute the similarity (or rather the distance) between couples of texts. This is achieved by computing text representations, just like in the previous recipe, for each text column before computing their distances using one of the following metrics:
	- **Cosine Distance**: which computes the `1 - cos(x, y)` where `x` and `y` are the sentences' word vectors.
	- **Euclidian Distance**: which is the classic `L2` distance between two vectors.
	- **Absolute Distance**: which is the classic `L1` distance between two vectors.
	- **Earth-Mover Distance**: a more complex ditance which can be informally seen as the minimum cost for turning one word vector into the other. For more details refer to the following [Wikipedia article](https://en.wikipedia.org/wiki/Earth_mover%27s_distance#EMD-based_similarity_analysis).

## Macro
### Download pre-trained embeddings
This macros downloads the specified model's pre-trained embeddings into the specified managed folder of the flow. In the folder doesn't exist, it creates it first then downloads the embeddings.

These are the available models:
- **Word2vec** (English)
- **GloVe** (English)
- **FastText** (English & French)
- **ELMo** (English)

**Note**: Unlike the other models, ELMo produces contextualized word embeddings. This means that the model will process the sentence where a word occurs to produce a context-dependent representation. As a result, ELMo embeddings are better but unfortunately also slower to compute.

## Recipes
### Compute sentence embeddings

This recipe creates sentence embeddings for the texts of a given column. The sentence embeddings are obtained using pre-trained word embeddings and one of the following two aggregation methods: a **simple average** aggregation (by default) or a weighted aggregation (so-called **SIF embeddings**).

**How to use the recipe**  
Using the recipe is very straightforward. After downloading the pre-trained word embeddings of your choice, just plug in your dataset and pre-trained vectors, select the column(s) containing your texts, an aggregation method and run the recipe!

**Note**: For SIF embeddings you can set advanced hyper-parameters such as the model's smoothing parameter and the number of components to extract.

**Note**: You can also use your own custom word embeddings. To do that, you will need to create a managed folder and put the embeddings in a text file where each line corresponds to a different word embedding in the following format: `word emb1 emb2 emb3 ... embN` where `emb` are the embedding values.
For example, if the word `dataiku` has a word vector ` [0.2, 1.2, 1, -0.6] ` then its corresponding line in the text file should be: `dataiku 0.2 1.2 1 -0.6`. 

### Compute sentence similarity

This recipe takes two text columns and computes the similarity (distance) of each couple of sentences. The similarity is based on sentence vectors computed using pre-trained word embeddings that are compared using one of three available metrics: cosine distance (default), euclidian distance (L2), absolute distance (L1) or earth-mover distance.

**How to use the recipe**  
Using this recipe is similar to using the "Compute sentence embeddings" recipe. The only differences are that you will now choose exactly two text columns and you will have the option to choose distance from the list of available distances.

### References

- **SIF** references:
> Sanjeev Arora, Yingyu Liang and Tengyu Ma, [*A Simple but Tough-to-Beat Baseline for Sentence Embeddings*](https://openreview.net/pdf?id=SyK00v5xx)
- **Word2vec** references:
> Tomas Mikolov, Kai Chen, Greg Corrado, and Jeffrey Dean.  [*Efficient Estimation of Word Representations in Vector Space*](http://arxiv.org/pdf/1301.3781.pdf). In Proceedings of Workshop at ICLR, 2013.
> 
> Tomas Mikolov, Ilya Sutskever, Kai Chen, Greg Corrado, and Jeffrey Dean.  [*Distributed Representations of Words and Phrases and their Compositionality*](http://arxiv.org/pdf/1310.4546.pdf). In Proceedings of NIPS, 2013.
> 
> Tomas Mikolov, Wen-tau Yih, and Geoffrey Zweig.  [*Linguistic Regularities in Continuous Space Word Representations*](http://research.microsoft.com/pubs/189726/rvecs.pdf). In Proceedings of NAACL HLT, 2013.

- **GloVe** references:
> Jeffrey Pennington, Richard Socher, and Christopher D. Manning. 2014.  [*GloVe: Global Vectors for Word Representation*](https://nlp.stanford.edu/pubs/glove.pdf).

- **FastText** references:
> P. Bojanowski, E. Grave, A. Joulin, T. Mikolov, [*Enriching Word Vectors with Subword Information*](https://arxiv.org/abs/1607.04606)

- **ELMo** references:
> Matthew E. Peters, Mark Neumann, Mohit Iyyer, Matt Gardner,  
Christopher Clark, Kenton Lee, Luke Zettlemoyer.  [*Deep contextualized word representations*](https://arxiv.org/abs/1802.05365) NAACL 2018.

### Licenses
... add licences for fasttext / word2vec / glove and ELMo.
