# Microsoft Azure Cognitive Services


## Plugin information

This plugin provides several tools to interact with the [Microsoft Azure Cognitive Services API](https://azure.microsoft.com/en-us/services/cognitive-services/), 
allowing to build intelligent algorithms into apps, websites, and bots so that they see, hear, speak, and understand 
the user needs through natural methods of communication.

It exposes two main sets of API's from the Azure Cognitive Services:

* **Computer Vision API**: The Computer Vision API provides state-of-the-art algorithms to process images and return information.
For example, it can be used to determine if an image contains mature content, or it can be used to find all the faces in an image. 
It also has other features like estimating dominant and accent colors, categorizing the content of images, and describing an image 
with complete English sentences. Additionally, it can also intelligently generate images thumbnails for displaying large images effectively.
[Read the documentation](https://westus.dev.cognitive.microsoft.com/docs/services/5adf991815e1060e6355ad44/operations/56f91f2e778daf14a499e1fc) 
for more information. 

* **Text Analytics API**: The Text Analytics API is a suite of text analytics web services built with best-in-class Microsoft machine learning algorithms. 
The API can be used to analyze unstructured text for tasks such as sentiment analysis, key phrase extraction and language detection. 
No training data is needed to use this API; just bring your text data. This API uses advanced natural language processing techniques to 
deliver best in class predictions. 
[Read the documentation](https://westus.dev.cognitive.microsoft.com/docs/services/TextAnalytics.V2.0/operations/56f30ceeeda5650db055a3c7) 
for more information.


## Using the Plugin

### Prerequisites
In order to use the Plugin, you will need:

* an Azure account
* proper credentials (access tokens) to interact with the service:
	1. Sign in to [Azure portal](https://portal.azure.com/).
	2. In the left navigation pane, select **All services**.
	3. In Filter, type Cognitive Services. Add the **Text Analytics** and/or the **Computer Vision** services depending on your use case
	4. Select a plan
* make sure you know in **which Azure region the services are valid**, the Plugin will need this information to get authenticated

### Plugin components
The Plugin has the following components:

* Text Analytics
	* [Language Detection](https://docs.microsoft.com/en-us/azure/cognitive-services/text-analytics/how-tos/text-analytics-how-to-language-detection): 
	evaluates text input and for each document and returns language identifiers with a score indicating the strength of the analysis. 
	Text Analytics recognizes up to 120 languages.
	* [Sentiment Analysis](https://docs.microsoft.com/en-us/azure/cognitive-services/text-analytics/how-tos/text-analytics-how-to-sentiment-analysis): 
	evaluates text input and returns a sentiment score for each document, ranging from 0 (negative) to 1 (positive). This capability 
	is useful for detecting positive and negative sentiment in social media, customer reviews, and discussion forums. 
	Content is provided by you; models and training data are provided by the service.
	* [Key Phrases Extraction](https://docs.microsoft.com/en-us/azure/cognitive-services/text-analytics/how-tos/text-analytics-how-to-keyword-extraction):
	evaluates unstructured text, and for each JSON document, returns a list of key phrases. This capability is useful if you need to quickly 
	identify the main points in a collection of documents. For example, given input text "The food was delicious and there were wonderful staff", 
	the service returns the main talking points: "food" and "wonderful staff".
	* [Named Entity Recognition](https://docs.microsoft.com/en-us/azure/cognitive-services/text-analytics/how-tos/text-analytics-how-to-entity-linking): 
	takes unstructured text, and for each JSON document, returns a list of disambiguated entities with links to more information on the web (Wikipedia and Bing).
	
* Computer Vision
	* [Image Analysis](https://westus.dev.cognitive.microsoft.com/docs/services/5adf991815e1060e6355ad44/operations/56f91f2e778daf14a499e1fa): 
	this operation extracts a rich set of visual features based on the image content. 
	* [Image Description](https://westus.dev.cognitive.microsoft.com/docs/services/5adf991815e1060e6355ad44/operations/56f91f2e778daf14a499e1fe):
	this operation generates a description of an image in human readable language with complete sentences. 
	The description is based on a collection of content tags, which are also returned by the operation. 
	More than one description can be generated for each image. Descriptions are ordered by their confidence score. All descriptions are in English. 
	* [Image Tagging](https://westus.dev.cognitive.microsoft.com/docs/services/5adf991815e1060e6355ad44/operations/56f91f2e778daf14a499e1ff):
	this operation generates a list of words, or tags, that are relevant to the content of the supplied image. 
	The Computer Vision API can return tags based on objects, living beings, scenery or actions found in images.
	
### Using the Plugin in Dataiku
Once installed, the Plugin is usable as a set of Custom Recipes. In all Recipes, you will be asked to enter your API key and the associated Azure region. 

* For Text Analytics: the Plugin expects an input **Dataset** with a column storing the text to analyze, 
and will output a new Dataset with the same schema augmented with a column storing the query response (or the error message, if any) in JSON format, 
to be parsed later using a Prepare recipe for instance.

* For Computer Vision: the Plugin expects an input **Filesystem Folder** storing all the images to analyze as files, 
and will output a Dataset storing information about the input image files and the query response (or the error message, if any) in JSON format, 
to be parsed later using a Prepare recipe for instance.

Each Recipe may have optional parameters corresponding to optional settings for the queries. 

## Contributing
You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests. Among potential improvements, you may want to add:

* support for other methods in the Text Analytics or Computer Vision services (ex: OCR)
* add support for other services