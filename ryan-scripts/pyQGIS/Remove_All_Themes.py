from qgis.core import QgsProject

# Get the current project instance
project = QgsProject.instance()

# Access the map theme collection
themes = project.mapThemeCollection()

# Get the list of all theme IDs
theme_ids = themes.mapThemes()

# Remove each theme by its ID
for theme_id in theme_ids:
    themes.removeMapTheme(theme_id)

print("All themes have been removed from the project.")
