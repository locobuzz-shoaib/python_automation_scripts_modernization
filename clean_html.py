import os
import glob


def clean_html_files(directory):
    """
    Removes all .html files in the specified directory.
    """
    html_files = glob.glob(os.path.join(directory, "*.html"))

    if not html_files:
        print("No HTML files found to clean.")
        return

    for html_file in html_files:
        try:
            os.remove(html_file)
            print(f"Deleted: {html_file}")
        except Exception as e:
            print(f"Failed to delete {html_file}: {e}")


if __name__ == "__main__":
    # Change this to the directory where your .html files are located
    directory = os.path.dirname(os.path.realpath(__file__))  # Current script directory
    clean_html_files(directory)
