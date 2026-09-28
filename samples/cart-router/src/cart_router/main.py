from ankka_flow import serve

from .router import CartRouter

if __name__ == "__main__":
    serve(CartRouter())
