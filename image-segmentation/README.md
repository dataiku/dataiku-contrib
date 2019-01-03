# Image segmentation Plugin

This plugin provides tools to perform object segmentation using Deep Learning. It comes with a recipe and a macro.

The model generates bounding boxes and segmentation masks for each instance of an object in the image. 
It's based on Feature Pyramid Network (FPN) and a ResNet101 backbone.

Semantic segmentation is understanding an image at pixel level i.e, we want to assign each pixel in the image an object class.

# Macro

### Download pre-trained detection model

This macro downloads a pre-trained model in your project. For now, only the COCO pre-trained Mask R-CNN 
obtained from the COCO dataset is available.

# Recipe

### Score image

This recipe takes the images, applies the segmentation and produces a json file with an associated score for every pixels of every images.
Optionally, it is possible to output the segmented images. 


# References

- Kaiming He, Georgia Gkioxari, Piotr Dollár, Ross Girshick,
[Mask R-CNN](https://arxiv.org/abs/1703.06870).

This plugin uses the implementation of Mask R-CNN on Python 3, Keras, and TensorFlow.
You can check the repository [here](https://github.com/matterport/Mask_RCNN/).
