## New ML engine in DSS
This plugin is superseeded by http://doc.dataiku.com/dss/latest/machine_learning/sparkling_water.html

## H2O plugin

This plugins provides H2O connectivity:
- the recipe `Build model` takes a dataset as input (and optionaly a validation dataset),
  trains a model, and saves it in a *folder*.
  This train dataset should be clean, DSS doesn't prepare it like it does for DSS models
  (e.g. it does not rescale any variable or drop those that look like an id).
- the recipe `Predict` takes such a folder as input, a dataset to score,
  and produces a new dataset with one column added: the predictions of the model.
  (Note that while the training set can be as big as H2O allows,
  the output dataset is currently exported via pandas and must fit in memory).

Usage:
- After installing the plugin, please click Administration → plugins → H2O → settings and fill the form.
- If your H2O instance is running on a Hadoop cluster, input datasets must be of type *HDFS* (to be accessible to each node. Otherwise you will get a “File foo does not exist” error.).
- If your H2O instance is running locally on the DSS host, input datasets must be of type *Filesystem* or *Upload* (this allows to locally train a model that doesn't fit in memory, and to demo the connectivity.)

For documentation about model parameters, see
- http://h2o-release.s3.amazonaws.com/h2o/rel-tibshirani/5/docs-website/h2o-py/docs/h2o.html#module-h2o.h2o
- or http://h2o-release.s3.amazonaws.com/h2o/rel-tibshirani/5/docs-website/h2o-docs/index.html#%E2%80%A6%20Building%20Models

The parameters `x`, `y` (for supervised models) and `validation_{x,y}` are handled by DSS:
their configuration is read from the input dataset and their path is passed to H2O.
Likewise, DSS chooses a `model_id`.

Mini H2O installation help
----
The links in <a href="http://h2o-release.s3.amazonaws.com/h2o/rel-slater/5/docs-website/h2o-docs/index.html#%E2%80%A6%20On%20Hadoop">official doc</a> are outdated. Instead go to http://h2o.ai → download → version 42. Follow the instructions for both Python and Hadoop.

Known bugs and future things we plan to implement
--------------
The partition id is invisible to h2o (i.e. there is no column containing it).

Unquoted, tab-separated input files containing commas might cause problems with H2O's
format guessing: error “Column separator mismatch. One file seems to use "," and the other uses "	".”.
We submitted a patch to H2O.

Algorithms GLRM, SVD and Naive_bayes are not supported for now because of a missing argument in h2o API.
We submitted a patch to h2o.
We might be able to provide a workaround in the meantime, please contact us for details.

For unquoted, tab-separated files, H2O considers several tabs as one single separator, and thus cannot handle empty columns.
Symptom: “Files conflict in number of columns.”.
Workaround: go to your input dataset → settings and set separator to something else than "\t", then rebuild it.
We did not submit a patch to H2O.

Aborting a DSS job does not cancel the corresponding H2O job. (Use for instance H2O's flow to do so.
The job should be easy to recognize thanks to the transparent name of its destination model.)
This can be changed once https://github.com/dataiku/dip/issues/3829 is closed.

Some arguments are not supported for now:
- training\_frame, offsets, weights, fold\_column (the arguments `nfold` and `fold_assignement` are supported).
- `beta_constraints` for algorithm GLM
- `init` for algorithm k-means when init is an H2OFrame. `init` is supported when it is a string.
