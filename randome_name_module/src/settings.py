from database import update_theme_in_db


def change_theme(style, theme_name):
    style.theme_use(theme_name)
    update_theme_in_db(theme_name)
