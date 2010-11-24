def parse_ip(text):
    text = text.strip()
    if ':' in text:
        host, port = text.rsplit(':', 1)
    elif '.' in text:
        host, port = text, 0
    else:
        host, port = '', text
    return host or '127.0.0.1', int(port)


def parse_ips(text):
    return map(parse_ip, text.split(','))



