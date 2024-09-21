import tkinter as tk
from io import BytesIO
from tkinter import messagebox

from ttkbootstrap import Style
from ttkbootstrap.widgets import Combobox  # Import Combobox from ttkbootstrap

from avatar_manager import get_avatar
from database import setup_db, save_name_in_db, load_saved_names, clear_all_names
from settings import change_theme
from utils import animate_selection, animate


def main():
    # Setup
    selected_theme = setup_db()
    root = tk.Tk()
    style = Style(theme=selected_theme)  # Modern theme selection using ttkbootstrap
    root.geometry("800x800")
    root.title("Random Name Selector with Avatars")

    # Header label
    header_label = tk.Label(root, text="Random Name Selector", font=("Helvetica", 26, "bold"))
    header_label.grid(row=0, column=0, columnspan=2, pady=20)

    # Frame for name entry and buttons
    frame = tk.Frame(root)
    frame.grid(row=1, column=0, columnspan=2, pady=20)

    entry_label = tk.Label(frame, text="Enter Name:")
    entry_label.grid(row=0, column=0, padx=10)
    entry = tk.Entry(frame, font=("Helvetica", 14))
    entry.grid(row=0, column=1, padx=10, pady=10)

    def add_person():
        person = entry.get()
        if not person:
            messagebox.showerror("Error", "Please enter a name")
            return
        if person not in people_listbox.get(0, tk.END):  # Ensure the name is unique
            try:
                avatar_image = get_avatar(person)
                avatar_bytes = BytesIO()
                avatar_image.save(avatar_bytes, format='PNG')
                save_name_in_db(person, avatar_bytes.getvalue())
                people_listbox.insert(tk.END, person)
                entry.delete(0, tk.END)  # Clear entry after adding
            except Exception as e:
                messagebox.showwarning("Error", str(e))
        else:
            messagebox.showwarning("Duplicate Name", "This name is already in the list.")

    add_button = tk.Button(frame, text="Add Person", command=add_person)
    add_button.grid(row=0, column=2, padx=10, pady=10)

    # People listbox and avatars
    people_listbox = tk.Listbox(root, height=10, width=50, font=("Helvetica", 14))
    people_listbox.grid(row=2, column=0, columnspan=2, pady=10)

    # Load saved names
    saved_names = load_saved_names()
    for name, avatar in saved_names:
        people_listbox.insert(tk.END, name)

    # Shayari label
    shayari_label = tk.Label(root, text="", font=("Helvetica", 18, "italic"), wraplength=500, justify="center")
    shayari_label.grid(row=3, column=0, columnspan=2, pady=10)

    # Theme selection using Combobox
    theme_var = tk.StringVar(value=selected_theme)

    def on_theme_change(event):
        theme = theme_var.get()
        change_theme(style, theme)

    theme_options = ['cyborg', 'darkly', 'superhero', 'flatly', 'journal']
    theme_menu = Combobox(root, textvariable=theme_var, values=theme_options, state="readonly")  # Use Combobox
    theme_menu.grid(row=4, column=0, columnspan=2, pady=10)
    theme_menu.bind("<<ComboboxSelected>>", on_theme_change)

    # Selected name label
    selected_label = tk.Label(root, text="", font=("Helvetica", 30, "bold"))
    selected_label.grid(row=6, column=0, columnspan=2, pady=20)

    # Button frame for random selection and clearing the list
    button_frame = tk.Frame(root)
    button_frame.grid(row=7, column=0, columnspan=2, pady=20)

    # Button to select a random person
    random_button = tk.Button(button_frame, text="Start Selection",
                              command=lambda: animate_selection(people_listbox, selected_label, root, shayari_label,
                                                                lambda: animate(people_listbox, selected_label, root,
                                                                                shayari_label)))
    random_button.grid(row=0, column=0, padx=20, pady=10)

    # Button to clear the list
    def clear_list():
        people_listbox.delete(0, tk.END)
        clear_all_names()  # Clear the database entries
        selected_label.config(text="")
        shayari_label.config(text="")  # Ensure Shayari is cleared as well

    clear_button = tk.Button(button_frame, text="Clear List", command=clear_list)
    clear_button.grid(row=0, column=1, padx=20, pady=10)

    # Toggle Theme Button (for light/dark mode)
    def toggle_theme():
        current_theme = style.theme_use()
        new_theme = 'flatly' if current_theme == 'darkly' else 'darkly'
        style.theme_use(new_theme)

    toggle_button = tk.Button(root, text="Toggle Light/Dark Mode", command=toggle_theme)
    toggle_button.grid(row=8, column=0, columnspan=2, pady=20)

    # Start the Tkinter loop
    root.mainloop()


if __name__ == "__main__":
    main()
