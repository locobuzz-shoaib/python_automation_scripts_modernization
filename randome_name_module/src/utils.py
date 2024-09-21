import random


def fade_in(label, name, root, step=0):
    """
    Utility function for fade-in animation of the selected name.
    """
    if step <= 1.0:
        color = f"#{int(step * 255):02x}{int(step * 127):02x}7f"
        label.config(text=name, foreground=color)
        root.after(50, fade_in, label, name, root, step + 0.05)
    else:
        label.config(text=name, foreground="#FF4500")


def animate_selection(people_listbox, selected_label, root, shayari_label, animate):
    """
    Start the animation for selecting a random person from the list.
    """
    if people_listbox.size() == 0:
        selected_label.config(text="No names in the list!", foreground="red")
        return

    global animation_running
    animation_running = True
    animate()

    # Update this line to pass 'shayari_label' as the fourth argument
    root.after(3000,
               lambda: stop_animation(people_listbox, selected_label, root, shayari_label))  # Stop after 3 seconds


def animate(people_listbox, selected_label, root, shayari_label):
    """
    Utility function to handle the animation of cycling through names.
    """
    if not animation_running:
        return
    current_names = people_listbox.get(0, "end")
    if current_names:
        next_name = random.choice(current_names)
        selected_label.config(text=next_name)

    root.after(100, animate, people_listbox, selected_label, root, shayari_label)


def stop_animation(people_listbox, selected_label, root, shayari_label):
    """
    Stop the animation and pick a random person with a fade-in effect and Shayari display.
    """
    global animation_running
    animation_running = False

    selected_person = random.choice(people_listbox.get(0, "end"))
    fade_in(selected_label, selected_person, root)

    # Pick a random Shayari from the list and display it
    from shayari_list import shayari_list
    shayari = random.choice(shayari_list)
    shayari_label.config(text=shayari)
