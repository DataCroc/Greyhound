import re


def extract_hashtags(text):
    return re.findall(r"#(\w+)", text)
