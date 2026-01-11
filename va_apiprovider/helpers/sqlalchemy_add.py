from ..constant import LINKTEMPLATE

def create_link_string(request, page, last_page, per_page):
    """Returns a string representing the value of the ``Link`` header.

    `page` is the number of the current page, `last_page` is the last page in
    the pagination, and `per_page` is the number of results per page.

    """
    linkstring = ''
    if page < last_page:
        next_page = page + 1
        linkstring = LINKTEMPLATE.format(request.url, next_page,
                                         per_page, 'next') + ', '
    linkstring += LINKTEMPLATE.format(request.url, last_page,
                                      per_page, 'last')
    return linkstring
