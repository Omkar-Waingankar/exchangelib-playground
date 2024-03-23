from exchangelib import Credentials, Configuration, Account, DELEGATE, FolderCollection
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def searchWithAllItems(account):
    all_items_folder = account.root / 'AllItems'
    matched_messages = all_items_folder.filter("kind:email").order_by('-datetime_received')[:10]
    return matched_messages

def searchAndInterleave(search_folders, query=""):
    all_messages = []

    for folder in search_folders:
        if query != "":
            matched_messages = folder.all().filter(query).order_by('-datetime_received')[:10]
            all_messages.extend(matched_messages)
        else:
            matched_messages = folder.all().order_by('-datetime_received')[:10]
            all_messages.extend(matched_messages)

    sorted_messages = sorted(all_messages, key=lambda msg: msg.datetime_received, reverse=True)[:10]
        
    return sorted_messages

def fetch_messages(folder, query=""):
    # Fetch and return messages for a single folder.
    # You can adjust the query here as needed.
    if query == "":
        return folder.all().order_by('-datetime_received')[:10]
    else:
        return folder.all().filter(query).order_by('-datetime_received')[:10]

def searchAndInterleaveConcurrent(search_folders, query=""):
    all_messages = []

    # Use ThreadPoolExecutor to fetch messages from each folder concurrently.
    with ThreadPoolExecutor(max_workers=len(search_folders)) as executor:
        # Create a future for each folder search.
        future_to_folder = {executor.submit(fetch_messages, folder, query): folder for folder in search_folders}
        
        for future in as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                # Get the result from the future.
                matched_messages = future.result()
                # Extend the all_messages list with the results from this folder.
                all_messages.extend(matched_messages)
            except Exception as exc:
                print(f'{folder.name} generated an exception: {exc}')
    
    # Sort all fetched messages by datetime_received and take the first 10.
    sorted_messages = sorted(all_messages, key=lambda msg: msg.datetime_received, reverse=True)[:10]
        
    return sorted_messages

# Set your email, password, and the server
email = 'test@nylas.info'
password = ''
server = 'east.EXCH092.serverdata.net'

# Set up credentials
credentials = Credentials(email, password)

# Manually specify the server configuration
config = Configuration(server=server, credentials=credentials)

# Set up the account with the manual configuration
account = Account(primary_smtp_address=email, config=config, autodiscover=False, access_type=DELEGATE)

# Make a list of known folders and set epoch start time
known_folders = [account.trash.name, account.inbox.name, account.sent.name, account.outbox.name, account.junk.name]

# Prepare a list of folders to search in
search_folders = [folder for folder in account.root.walk() if folder.name in known_folders]

print("SEARCHING WITH INTERLEAVING CONCURRENTLY")
searchTimes = []
for i in range(10):
    start = time.time()
    matched_messages = searchAndInterleaveConcurrent(search_folders)

    ids = []
    for msg in matched_messages:
        ids.append(msg.id)
    print(f"Found {len(ids)} messages")

    end = time.time()
    print(f"Search Time: {end - start}")
    searchTimes.append(end - start)
print(f"Search Times: {searchTimes}")
print(f"Average Search Time: {sum(searchTimes) / len(searchTimes)}")

