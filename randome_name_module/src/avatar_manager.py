from io import BytesIO

import requests
from PIL import Image


# Function to fetch an avatar from the Multiavatar API based on the name
def get_avatar(name):
    """
    Fetches an avatar image from the Multiavatar API based on the name.
    Returns a PIL image object.
    """
    url = f"https://api.multiavatar.com/{name}.png"
    response = requests.get(url)

    # Open the image from the response content
    image = Image.open(BytesIO(response.content))
    return image
