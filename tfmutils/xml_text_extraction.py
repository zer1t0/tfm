import bs4

### ========= XML text extraction ===========

def extract_texts_from_xml(xml_str):
    soup = bs4.BeautifulSoup(xml_str, "xml")
    return _extract_texts_from_xml_soup(soup)

def _extract_texts_from_xml_soup(soup):
    msg = []
    for c in soup.children:
        if isinstance(c, bs4.Tag):
            msg.append(c.name)
            msg.extend(_extract_texts_from_xml_soup(c))
        elif isinstance(c, bs4.NavigableString) \
             and not isinstance(c, bs4.Comment):
            msg.append(c)

    return msg

