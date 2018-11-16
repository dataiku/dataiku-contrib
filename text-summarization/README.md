# Text Summarization

This plugin provides a tool for doing automatic text summarization. It uses extractive summarization methods, which means that the summary will be a number of extracted sentences from the original input text. This can be used for example to summarize customer reviews or long reports into shorter texts.

The plugin comes with a single recipe that uses one of three possible models:

- **Text Rank**[`[1]`](https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf), which builds a graph of every sentence of the input text, where each text is linked to its most similar sentences, before running the PageRank algorithm to select the most relevent sentences for a summary.

- **KL-Sum**[`[2]`](http://www.aclweb.org/anthology/N09-1041), which summarizes texts by decreasing a KL Divergence criterion. In practice, it selects sentences based on how much they have the same word distribution as the original text. 

- **LSA**[`[3]`](http://www.kiv.zcu.cz/~jstein/publikace/isim2004.pdf), which uses Latent Semantic Allocation (LSA) to summarize texts. Basically, this starts by looking for the most important topics of the input text then keeps the sentences that best represent these topics.

## Recipes
### Summarize Texts

This recipe summarizes your texts using one of the available summarization techniques (Text Rank, KL-Sum and LSA).

**How to use the recipe**
Using the recipe is straightforward. First plug in your dataset and select the column containing your texts. Then, select a method, set the number of desired sentences and run the recipe!

### References
>Rada Mihalcea and Paul Tarau, [*TextRank: Bringing Order into Texts*](https://web.eecs.umich.edu/~mihalcea/papers/mihalcea.emnlp04.pdf).
>
>Aria Haghighi and Lucy Vanderwende, [*Exploring Content Models for Multi-Document Summarization*](http://www.aclweb.org/anthology/N09-1041).
>
>Josef Steinberger and Karel Ježek, [*Using Latent Semantic Analysis in Text Summarization and Summary Evaluation*](http://www.kiv.zcu.cz/~jstein/publikace/isim2004.pdf).
