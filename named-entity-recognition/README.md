# Named Entity Recognition

This plugin provides a tool for extracting Named Entities (i.e. People names, Dates, Places, etc) which can be useful for extracting knowledge from your texts.

The plugin comes with a single recipe that extracts entities using one of two possible models: 
	- **SpaCy**: a faster but slightly less precise model. Another advantage of SpaCy is its support for many languages.
	-  **Flair**: a slower but more precise model for Named Entity Recognition.

**Note 1**: In order to successfully download and use SpaCy you will need to have admin rights over the plugin code environment.

**Note 2**: Since Flair's model is quite large, the plugin proposes a macro for downloading the model into a managed folder. This folder can then be used with the recipe to extract entities using Flair.

## Recipes
### Extract Named Entities

This recipe extracts named entities such as **LOC** (localisation) and **PER** (person) from your texts. The default model is SpaCy which is available for both English and French. To use a more precise (but slower) model, choose Flair.

**How to use the recipe**  
Using the recipe is straightforward. Just plug in your dataset, select the column containing your texts and run the recipe!

Optionally, you can set some advanced settings. For example, you can choose Flair (only available in English) for a more precise extraction. You can also choose the format in which the extracted entities are presented: a separate column for each entity type (default) or a single column with a JSON containing all the entities.

## WebApp templates
### SpaCy WebApp
This plugin offers a WebApp template for testing SpaCy's NER model. To successfully run the webapp you will need to:

- Create a Standard WebApp using the template, then enable a python backend.
- Create a python code environment following these requirements:

	```
	flask
	spacy
	```

- In the WebApp's settings page, select the previously created code environment and activate `Bootsrap`.

### References
Alan Akbik, Duncan Blythe and Roland Vollgraf [*Contextual String Embeddings for Sequence Labeling, 2018*](http://alanakbik.github.io/papers/coling2018.pdf) In 27th International Conference on Computational Linguistics.
