import requests
import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, urlsplit

# Check if a URL is absolute
def is_absolute_url(url):
    common_schemes = ("http://", "https://", "ftp://", "ftps://")
    return any(url.startswith(scheme) for scheme in common_schemes)

# Normalize URL by removing '/'
def normalize_url(url):
    if url.endswith("/"):
        url = url[:-1]
    return url

# Insert a URL to the database
def insert_url_to_db(cursor, url, depth, secure):
    cursor.execute("INSERT INTO page_url (url, depth, secure) VALUES (?, ?, ?)", (url, depth, secure))

# Get all absolute links from a page
def get_links(url, depth, cursor, conn):
    try:
        # HTTP GET request to get HTML content
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the HTML content to a BeautifulSoup object
            soup = BeautifulSoup(response.text, 'html.parser')

            # Check URL security
            secure = 1 if url.startswith("https") else 0
            # Save the current URL
            normalized_url = normalize_url(url)  # Normalize the URL
            insert_url_to_db(cursor, normalized_url, depth, secure)

            # Link extraction is limited to a depth of 3 levels
            if depth < 3:
                # Retrieve all links on the page
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link.get('href')
                    # Check if the URL is absolute, belongs to the xmco.fr domain, and is not already recorded
                    if "mailto:" not in href and not href.startswith("#") and is_absolute_url(href) and "www.xmco.fr" in href and not cursor.execute("SELECT 1 FROM page_url WHERE url = ?", (normalize_url(href),)).fetchone():
                        # Get links from the new URL + increase depth
                        get_links(href, depth + 1, cursor, conn)

    except Exception as e:
        print(f"Error while retrieving URL {url}: {e}")

if __name__ == '__main__':
    # Create the SQLite database
    conn = sqlite3.connect('xmco_links.db')
    cursor = conn.cursor()

    # Clear the table content if it exists
    #cursor.execute("DROP TABLE IF EXISTS page_url")
    #conn.commit() 

    # Create the table
    cursor.execute("CREATE TABLE IF NOT EXISTS page_url (id INTEGER PRIMARY KEY, secure INTEGER, depth INTEGER, url TEXT)")
    conn.commit() 
    
    # Starting URL
    start_url = "https://www.xmco.fr/"
    get_links(start_url, 1, cursor, conn)

    # Display the contents of the database
    cursor.execute("SELECT * FROM page_url")
    print("| ID | URL | Depth | Secure |")
    for row in cursor.fetchall():
        print(f"| {row[0]} | {row[3]} | {row[2]} | {row[1]} |")

    conn.close()
