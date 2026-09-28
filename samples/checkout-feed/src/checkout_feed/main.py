from ankka_flow import serve

from .feed import CheckoutFeed

if __name__ == "__main__":
    serve(CheckoutFeed())
