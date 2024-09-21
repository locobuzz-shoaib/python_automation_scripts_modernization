import requests
from PIL import Image
from io import BytesIO

# Function to fetch the avatar from the Multiavatar API
def fetch_avatar_from_multiavatar(name):
    url = f"https://api.multiavatar.com/{name}.png"
    response = requests.get(url)
    avatar_image = Image.open(BytesIO(response.content))
    return avatar_image

# Function to fetch the avatar from the DiceBear API
def fetch_avatar_from_dicebear(name, avatar_type="male"):
    url = f"https://avatars.dicebear.com/api/{avatar_type}/{name}.png"
    response = requests.get(url)
    avatar_image = Image.open(BytesIO(response.content))
    return avatar_image

# Function to get an avatar based on the name
def get_avatar(name):
    # Randomly choose an avatar from Multiavatar or DiceBear
    return fetch_avatar_from_multiavatar(name)
