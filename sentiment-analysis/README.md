# Sentiment Analysis Plugin

This plugin provides a tool for performing Sentiment Analysis on textual data.

The plugin comes with a single recipe that allows you to estimate the sentiment polarity (positive/negative) of a text based on it content. For instance, you could use this recipe on a dataset of user reviews or social media data (such as tweets) to know which instances are positive (category 1) and which are negative (category 0).

## Recipes
### Compute sentiment scores

Classifies your texts into two categories : 1 when the sentiment is positive (e.g. "This movie is good") and 0 otherwise.

**How to use the recipe**  
Using the recipe is very straightforward. Just plug in your dataset, select the column containing the texts you are interested in scoring and run the recipe! The plugin automatically detects the language of your texts and uses a pre-trained model to predict their sentiment polarities.

(*optional*) You can tick the “Advanced settings” box to manually set some advanced settings. This can be used to manually set the text language and/or output the model confidence for each prediction.

### References

- P. Bojanowski, E. Grave, A. Joulin, T. Mikolov, [*Enriching Word Vectors with Subword Information*](https://arxiv.org/abs/1607.04606)

- A. Joulin, E. Grave, P. Bojanowski, T. Mikolov, [*Bag of Tricks for Efficient Text Classification*](https://arxiv.org/abs/1607.01759)

- A. Joulin, E. Grave, P. Bojanowski, M. Douze, H. Jégou, T. Mikolov, [*FastText.zip: Compressing text classification models*](https://arxiv.org/abs/1612.03651)

### Licenses
The Language detection model bundled with this plugin comes from fastText, and is distributed under the [*Creative Commons Attribution-Share-Alike License 3.0*](https://creativecommons.org/licenses/by-sa/3.0/).

The English model bundled with this plugin comes also from fastText, which is is [*BSD-licensed*](https://github.com/facebookresearch/fastText/blob/master/LICENSE).
