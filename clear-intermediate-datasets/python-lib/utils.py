def populate_result_table_with_list():
    # TODO
    # Populate result table from list with arbitrary table size
    return NotImplementedError()

def append_datasets_to_list(recipe_dict, list_to_append):
    # Append datasets' names from recipe's input or output dictionaries to an input or output list respectively
    for key in recipe_dict:
        list_to_append += [x["ref"] for x in recipe_dict[key]["items"] if x["ref"] not in list_to_append]
