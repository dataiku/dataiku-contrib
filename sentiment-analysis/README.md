# Sentiment Analysis Plugin

This plugin provides a tool for performing Sentiment Analysis on textual data.

The plugin comes with a single recipe that allows you to estimate the sentiment polarity (positive/negative) of a text based on it content. For instance, you could use this recipe on a dataset of user reviews or social media data (such as tweets) to know which instances are positive (category 1) and which are negative (category 0).

## Recipes
### Compute sentiment scores

Classifies your texts into two categories : 1 when the sentiment is positive (e.g. "This movie is good") and 0 otherwise.

**How to use the recipe**  
Using the recipe is very straightforward. Just plug in your dataset, select the column containing the texts you are interested in scoring and run the recipe!

[Option] You can tick the “Output confidence scores” box to output the model's confidence for each prediction.

[Option] You can un-tick the “Predict polarity” box to produce sentiment scores that vary from 1 (highly negative) to 5 (highly positive).


### References

- P. Bojanowski, E. Grave, A. Joulin, T. Mikolov, [*Enriching Word Vectors with Subword Information*](https://arxiv.org/abs/1607.04606)

- A. Joulin, E. Grave, P. Bojanowski, T. Mikolov, [*Bag of Tricks for Efficient Text Classification*](https://arxiv.org/abs/1607.01759)

- A. Joulin, E. Grave, P. Bojanowski, M. Douze, H. Jégou, T. Mikolov, [*FastText.zip: Compressing text classification models*](https://arxiv.org/abs/1612.03651)

This plugin uses the text classification library [fastText](https://fasttext.cc/). If you are interested in learning more about how fastText can be used for text classification, you can refer to [the following tutorial](fasttext_tutorial/fastText.ipynb).

### Licenses
The English model bundled with this plugin comes from fastText, which is is [*BSD-licensed*](https://github.com/facebookresearch/fastText/blob/master/LICENSE).
