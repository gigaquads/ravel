from {{ project_name|snake }}.app import App


# instantiate the global app instance here instead of in the app.py module in
# order to avoid side-effects when importing the App class outside the context
# of running it as a WSGI app.
app = App()
