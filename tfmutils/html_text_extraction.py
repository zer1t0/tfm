import bs4

### ========= HTML text extraction ===========

NO_MAIN_CONTENT_TAGS = [
    "script",
    "style",
    "meta",
    "link",
    "nav",
    "header",
    "footer",
    "aside",
    "address",
    "ul",
    "ol",
    "form",
    "iframe",
    "svg",
]

NO_MAIN_CONTENT_CLASSES = [
    "header",
    "footer",
    "sidebar",
    "sidesection",
    "leftbar"
]

def extract_title_and_texts_from_html(html_str):
    soup = bs4.BeautifulSoup(html_str, "lxml")

    title = soup.title.text if soup.title else ""
    for tag_name in NO_MAIN_CONTENT_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    remove_non_main_divs(soup)
            
    return title, _extract_texts_from_html_soup(soup)

def remove_non_main_divs(soup):
    for div in soup.find_all("div"):
        if div is None or div.descomposed:
            continue

        try:
            div_id = div["id"]
            if any(n in div_id for n in NO_MAIN_CONTENT_CLASSES):
                div.decompose()
                continue
        except (KeyError, TypeError):
            pass

        try:
            div_classes = " ".join(div["class"])
            if any(n in div_classes for n in NO_MAIN_CONTENT_CLASSES):
                div.decompose()
                continue
        except (KeyError, TypeError):
            pass

def _extract_texts_from_html_soup(soup):

    p_texts = soup.findAll(text=True)
    filtered_texts = list(filter(_is_tag_visible, p_texts))
    return filtered_texts

def _is_tag_visible(element):

    if isinstance(element, bs4.Comment):
        return False
        
    if element.parent.name in ['span']:
        return True

    if element.parent.name in [
            'head',
            '[document]',
            "script",
            "style",
            "meta",
            "link"
    ]:
        return False
    
    return True




# ========== Text extraction from CrawlerScraper ============
# https://github.com/hamzatartori/CrawlerScraper/

# def CrawlScrape_text_extraction(html_str):
#     soup = bs4.BeautifulSoup(html_str, "html.parser")
    
#     p_texts = soup.findAll(text=True)
#     filtered_prev_texts = list(filter(tag_visible, p_texts))
#     cleaned_prev_text = [x for x in filtered_prev_texts if x]
#     prev_text = [x for x in cleaned_prev_text if x != ' ']
#     prev_text = [x for x in prev_text if x != '\n']

#     return prev_text

# def tag_visible(element):
#     """
#     :param element: Beautifulsoup element (html tag)
#     :return: whether the given element contains a visible text in the webpage
#     """

#     # print(type(element))
    
#     if element.parent.name in ['span']:
#         return True
#     if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
#         return False
#     if isinstance(element, bs4.Comment):
#         print("This is a comment:", element)
#         return False
    
#     return True
