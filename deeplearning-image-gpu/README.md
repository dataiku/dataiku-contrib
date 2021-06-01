## Plugin information

⚠️  Starting with version 0.1.7, this plugin is considered as "Legacy" and will be maintained only to fix bugs.
For the latest features, we recommend using the [new Deep Learning plugin](https://www.dataiku.com/product/plugins/deeplearning-image/)

This plugin provides several tools to use images in machine learning applications. You can use a pre trained model to score images and obtain classes, or for feature extraction (obtaining the values taken by a layer for each image). You can also retrain a model to specialize it on a particular set of images, this process is known as transfer learning.

This plugin relies on the Keras library. Keras is an open source neural network library written in Python. We use it to run on top of the TensorFlow library as it enables fast experimentation with deep neural networks.

This plugin provides a total of 3 recipes, a macro and a webapp template.

This plugin will require the tensorflow-gpu package and is able to run computations on GPUs.

## Copyright notice
Original work Copyright (c) 2016 François Chollet
