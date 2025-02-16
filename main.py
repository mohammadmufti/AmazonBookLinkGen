import pandas as pd
import urllib.parse
from typing import Optional
import re
import requests
from time import sleep
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import backoff


def clean_text(text: str) -> str:
    """Clean text by removing special characters and normalizing spaces."""
    if pd.isna(text):
        return ""
    return re.sub(r'\s+', ' ', str(text).strip())


def is_valid_isbn(isbn: str) -> bool:
    """Validate ISBN-10 or ISBN-13 format."""
    isbn = re.sub(r'[-\s]', '', isbn)
    if len(isbn) == 10:
        return bool(re.match(r'^\d{9}[\dX]$', isbn))
    elif len(isbn) == 13:
        return bool(re.match(r'^\d{13}$', isbn))
    return False


def get_session():
    """Create a session with rotating user agents."""
    session = requests.Session()
    ua = UserAgent()

    session.headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'TE': 'Trailers',
        'DNT': '1'
    }
    return session


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException, Exception),
                      max_tries=5)
def make_request(url: str, session: requests.Session) -> requests.Response:
    """Make a request with exponential backoff and rotating user agents."""
    # Add random delay between requests (2-7 seconds)
    sleep(random.uniform(2, 7))

    # Update user agent for each request
    session.headers.update({'User-Agent': UserAgent().random})

    response = session.get(url, timeout=10)

    # If we get a 503, wait longer and retry
    if response.status_code == 503:
        print("  Received 503 error, waiting longer before retry...")
        sleep(random.uniform(10, 15))
        raise requests.exceptions.RequestException("503 error")

    return response


def create_search_url(row: pd.Series) -> str:
    """Create a search URL based on book information."""
    search_parts = []

    if not pd.isna(row['Book Title']) and str(row['Book Title']).strip():
        search_parts.append(clean_text(row['Book Title']))

    if not pd.isna(row['Author']) and str(row['Author']).strip():
        search_parts.append(clean_text(row['Author']))

    if not pd.isna(row['Yr']) and str(row['Yr']).strip():
        search_parts.append(str(row['Yr']))

    if not pd.isna(row['Ed']) and str(row['Ed']).strip():
        edition = str(row['Ed']).strip()
        if edition.lower().endswith('th'):
            search_parts.append(f"{edition} edition")

    if not search_parts:
        return ""

    search_query = " ".join(search_parts)
    encoded_query = urllib.parse.quote(search_query)
    return f"https://www.amazon.com/s?k={encoded_query}&i=stripbooks"


def get_first_product_link(search_url: str, book_title: str, session: requests.Session) -> str:
    """Get the first product link from Amazon search results."""
    try:
        print(f"  Searching for: {book_title}")

        response = make_request(search_url, session)
        if response.status_code != 200:
            print(f"  Search failed with status code: {response.status_code}")
            return search_url

        soup = BeautifulSoup(response.text, 'html.parser')

        # Try different CSS selectors for product links
        selectors = [
            'a.a-link-normal.s-no-outline',
            'a.a-link-normal.a-text-normal',
            'div.s-result-item h2 a',
            'div[data-component-type="s-search-result"] h2 a',
            'div.s-result-item a.a-link-normal'
        ]

        for selector in selectors:
            product_links = soup.select(selector)
            for link in product_links:
                href = link.get('href', '')
                if '/dp/' in href or '/gp/product/' in href:
                    product_id = re.search(r'/(dp|gp/product)/([A-Z0-9]{10})', href)
                    if product_id:
                        clean_url = f"https://www.amazon.com/dp/{product_id.group(2)}"
                        print(f"  Found product link: {clean_url}")
                        return clean_url

        print("  No valid product link found in search results")
        return search_url

    except Exception as e:
        print(f"  Error getting product link: {str(e)}")
        return search_url


def process_books_csv(input_file: str, output_file: str) -> None:
    """Process the books CSV file and add Amazon links."""
    try:
        print(f"Reading input file: {input_file}")
        try:
            df = pd.read_csv(input_file, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8 encoding failed, trying latin-1")
            df = pd.read_csv(input_file, encoding='latin-1')

        print(f"Successfully loaded {len(df)} records")

        # Create a session for all requests
        session = get_session()

        def process_book(row):
            book_title = clean_text(row['Book Title'])
            print(f"\nProcessing book #{row.name + 1}: {book_title}")

            # Try ISBN/UPC first
            if not pd.isna(row['UPC']) and str(row['UPC']).strip():
                isbn = str(row['UPC']).strip()
                if is_valid_isbn(isbn):
                    print(f"  Found valid ISBN: {isbn}")
                    direct_url = f"https://www.amazon.com/dp/{isbn}"
                    try:
                        response = make_request(direct_url, session)
                        if response.status_code == 200 and 'dp/' in response.url:
                            print("  ISBN link valid")
                            return direct_url
                        print("  ISBN link invalid, trying search")
                    except:
                        print("  Error checking ISBN link, trying search")

            # Fall back to search and get first product
            search_url = create_search_url(row)
            if search_url:
                return get_first_product_link(search_url, book_title, session)
            return ""

        # Process books and add Amazon link column
        print("\nGenerating and validating Amazon links...")
        df['Amazon_Link'] = df.apply(process_book, axis=1)

        # Save to new CSV file
        print(f"\nSaving output to: {output_file}")
        df.to_csv(output_file, index=False, encoding='utf-8')

        # Print summary statistics
        total_books = len(df)
        books_with_links = len(df[df['Amazon_Link'].str.len() > 0])
        books_with_product_links = len(df[df['Amazon_Link'].str.contains('/dp/')])

        print(f"\nProcessing complete:")
        print(f"Total books processed: {total_books}")
        print(f"Direct product links generated: {books_with_product_links}")
        print(f"Search links generated: {books_with_links - books_with_product_links}")
        print(f"Failed to generate links: {total_books - books_with_links}")

        # Print books with no links
        failed_books = df[df['Amazon_Link'].str.len() == 0]
        if not failed_books.empty:
            print("\nBooks with no links generated:")
            for idx, row in failed_books.iterrows():
                print(f"- {row['Book Title']}")

    except Exception as e:
        print(f"Error processing file: {str(e)}")


if __name__ == "__main__":
    input_file = "BooksInput.csv"
    output_file = "BooksOutput.csv"
    process_books_csv(input_file, output_file)
