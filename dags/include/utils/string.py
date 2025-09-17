import re


def is_lower(val):
    return val == val.lower()


def is_upper(val):
    return not is_lower(val)


def camel_to_snake(val: str) -> str:
    bad_chars = " -_/"
    bad_char_trans = str.maketrans(bad_chars, len(bad_chars) * '_')
    if re.search('|'.join(bad_chars), val):  # value is not camel case, just make lowercase
        return val.translate(bad_char_trans).lower()
    if val == val.upper():
        return val.lower()
    if val[0] == val[0].lower():
        val = val[0].upper() + val[1:]

    tokens = re.findall(r'[A-Z]+', val)
    words = []

    def split_token(token):
        return token[:-1], token[-1:]

    for idx, curr_token in enumerate(tokens):
        try:
            next_token = tokens[idx + 1]
        except IndexError:
            next_token = None
        if len(curr_token) > 1:
            if len(curr_token) == len(val):
                words.append(curr_token)
                continue
            else:
                curr_word, curr_token = split_token(curr_token)
                words.append(curr_word)
                val = val[len(curr_word) :]
        next_token_position = val[1:].find(next_token) + 1 if next_token else None
        curr_word = val[:next_token_position]
        words.append(curr_word)
        val = val[next_token_position:]

    return '_'.join(words).lower()


def unindent(val: str, char_count: int = 8) -> str:
    return re.sub(rf"^[ ]{{{char_count}}}", "", val, flags=re.MULTILINE)


def unindent_auto(val: str, tab_width=4) -> str:
    val = val.replace("\t", tab_width * " ")
    initial_match = re.match(r"(?:(\s*?\n)?)([\w\W]+)", val, re.MULTILINE)
    if not initial_match:
        raise Exception("no initial match")
    blank_line, body = initial_match.groups()
    body_match = re.match(r"\s*", body, re.UNICODE)
    if not body_match:
        raise Exception("no match in body")
    indent_level = len(body_match.group(0))
    new_val = blank_line and f"\n{body}" or body
    return unindent(new_val, char_count=indent_level)


def indent(cmd: str, char_count=4) -> str:
    return cmd.replace("\n", "\n" + char_count * " ")
