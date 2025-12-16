def clean_data_decimal(value):
    if value is None:
        return None
    
    value = str(value).strip()

    if value.endswith(".0"):
        value = value[:-2]

    # Remove any accidental whitespace
    value = value.strip()

    # Remove invalid values
    if value in ["", "nan", "None"]:
        return None
    if len(value) > 1 and value[0] =='0':
        return value[1:]

    return value