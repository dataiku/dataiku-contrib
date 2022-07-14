def populate_result_table_with_list():
    # TODO
    # Populate result table from list with arbitrary table size
    return NotImplementedError()

def append_datasets_to_list(recipe_dict, list_to_append):
    """
    Append datasets' names from recipe's input or output dictionaries to an input or output list respectively
    """
    for key in recipe_dict:
        list_to_append += [x["ref"] for x in recipe_dict[key]["items"] if x["ref"] not in list_to_append]

def append_dropdown_choices(list_to_parse, key_to_use):
    """
    Parses through the list of projects in an instance, or a list of datasets in a project
    to return the list as choices of a dropdown in the runnable.json
    """
    choices = []
    for item in list_to_parse:
        choices.append({"value": item.get(key_to_use), "label": item.get(key_to_use)})
    return choices
