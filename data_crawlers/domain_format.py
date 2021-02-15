from urllib.parse import urlparse
import re


def parse_url(user_input):
    """
    :param user_input: URL that user inputs
    :return: URL with domain name and extension - domain.com
    """
    parsed = urlparse(user_input)
    if parsed.scheme:
        url = re.sub('www.', '', parsed.netloc)
        return url.lower()
    else:
        url = re.sub('www.', '', parsed.path)
        return url.lower()


def column_count(row):
    """
    Function for counting how many columns are filled for a particular merchant scan.
    Returned result is used to display for customer how much of the 9 requisites have
    been detected for particular merchant.
    :param row - single merchant.requisite row
    """
    count = 0
    for column in row.__dict__:
        if row.__dict__[column] and column != '_sa_instance_state' and column != 'merchant_id' \
                and column != 'id' and column != 'website' and row.__dict__[column] != 'None':
            count += 1

    return count
