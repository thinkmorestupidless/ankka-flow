from ankka_flow import serve

from .streamlet import Echo

if __name__ == "__main__":
    serve(Echo())
