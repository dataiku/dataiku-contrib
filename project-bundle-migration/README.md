# Dataiku Bundle Project and Push to Automation Node Plugin

Current Version: 0.0.1

Status:  **experimental**  

## Overview

This bundles a project and imports it onto another Dataiku automation instance.

## Before Running this Plugin

1. Make sure that any external connections used in the initial project also exist on the automation node instance. These connections must have the same names, but do not have to point to the same paths.

2. If you use any datasets from the server filesystem or that you uploaded manually, you must manually "attach" these datasets to all bundles (just once, not each time you create a bundle).

To do this, click on the "Bundles" tab of the project, then "More", then "Configure Bundles Content".

![Configure_bundles_content](doc/Configure_bundles_content.png)

Add all uploaded files, server_filesystem files, and deployed models using the +Add button.

![Add_objects_to_bundle](doc/Add_objects_to_bundle.png)

3. Create a Global API key on the target automation instance, which should be used as an input for this macro.
