# Causal Inference Web app

## Introduction

Traditional Machine learning methods are concerned with predicting an outcome given explanatory variables.
However, estimating the (causal) effect of one or several variables on the outcome variable lies beyond the scope of those models -- they can't answer "what if" questions.

To see this, suppose you are trying to predict drowning probabilities in some region. You collect many past drowning incidents data that you cross with many other data sources. Your model will probably show a strong and positive effect of temperature on drowning and a strong a positive effect of fan purchases on drowning. Yet, nobody would actually buy that drownings are caused by fan purchases and would be quick to understand that fan purchases is caused by temperature as temperature is also a cause for going swimming which in turn increases risks of drowning. What happens is that we have a clear model of which variable causes another and are therefore able to dissociate correlation from causation.

In the above example, one easily rules out fan purchases as a cause of drowning. This is because we all mentally formulate a "causal model" of which variable causes which. This allows us not to mistake the results of our machine learning for causal effects. Three questions can then be asked:
1) How can we write down this mental model?
2) How can we use this model to see whether we can recover the causal effect of one variable onto another with our data?
3) Can we estimate this causal effect and if so, under which assumptions?

The answer to question 1) resides in drawing a graph.

Question 2) is answered by the do-calculus as presented by J. Pearl (see reference below), which is leveraged in this plugin through to the <a href="https://github.com/Microsoft/dowhy">dowhy python library</a> which analyzses the graph and, in light of data, tells us whether or not the effect is identifiable.

For Question 3), this plugin determines whether the OLS estimate of a linear model coefficient has a causal interpretation and whether there exists instrumental variables estimate the same coefficient via TSLS. In either case, assumptions used are made explicit. This is once again done though the dowhy package. For more information on the theory behind causal inference, please refer to The Book of Why by Judea Pearl's or The Elements of Causal Inference by Bernhard Schölkopf, Dominik Janzing, and Jonas Peters.

## How to install

Install this plugin into DSS by cloning `https://github.com/dataiku/dataiku-contrib` with path `dowhy-webapp` (in Plugins > Advanced).

You will then have to create a DSS managed Python >= 3.6 code-env, and install all the packages from the `requirements.txt`:
```
sklearn==0.0
networkx==2.1
matplotlib==2.2.2
sympy==1.2
Flask==1.0.2
pygraphviz==1.3.1
git+git://github.com/Microsoft/dowhy.git
```

You can now create a new webapp using this one as a template, and set the code-env to the one you just created. Voilà!

## Usage and limitations

- Select the dataset that you want to study
- Build your graph by either inputting a pair of nodes (Node A points into Node B) or by directly copying the content of a <a href="https://en.wikipedia.org/wiki/DOT_(graph_description_language)">DOT graph file</a> into the text area. For example you can copy and paste, something like "a"->"b"; "a"->"c"->"d"; (but not 'digraph {"a"->"b"; "a"->"c"->"d"}'). The Graph must be Directed and Acyclic where each vertex represents either an observed feature (column in your dataset) or unobserved feature and each edge going from X to Y means that X causes Y.
- Select the Treatment and Outcome pair of variables with the goal of studying the causal effect that treatment has on the outcome variable.
- Click on Visualize Graph to visualize the graph from the text area or click on Register Graph to both visualize the graph and get results from the dowhy library
