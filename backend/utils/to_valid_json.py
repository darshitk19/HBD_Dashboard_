# this function is used to convert the list type of data present in string form to convert it back into list type and then to the JSON type
import json
import ast
import pandas as pd
def to_valid_json(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        # Convert string val to list type and then the list type is converted into JSON type
        return json.dumps(ast.literal_eval(val))
    except:
        return None